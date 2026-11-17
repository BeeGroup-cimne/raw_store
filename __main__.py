import argparse
import time
import beelib
import load_dotenv
import logging
import os
import signal
import pandas as pd
import hashlib

from logging_setup import setup_logging

logger = logging.getLogger(__name__)


def transform_data_ts(items, ts_column, hash_column, seq_column=None):
    years = [pd.to_datetime(x[ts_column], unit="s").year for x in items]
    new_items = []
    for i, it in enumerate(items):
        it_new = {
            'year': years[i],
            'hash': it.pop(hash_column),
            'ts': it.pop(ts_column)
        }
        if seq_column:
            it_new['seq'] = int(it.pop(seq_column))
        for key, val in it.items():
            it_new[key] = str(val)
        new_items.append(it_new)
    return new_items


def get_ts_options(with_seq=False):
    return {
        'partition_rows': {
            'rows': ['hash', 'year'],
            'types': ['text', 'int']
        },
        'sort_row': {
            'rows': ['ts', 'seq'] if with_seq else ['ts'],
            'types': ['int', 'int'] if with_seq else ['int']
        },
        'columns': {
            'info': ['all'],
        }
    }


DEFAULT_BUCKETS = 100       # el valor fix d'abans: els missatges sense `buckets` continuen anant aquí


def transform_data_static(items, id_column, buckets=DEFAULT_BUCKETS):
    ids = [int(hashlib.md5(x[id_column].encode()).hexdigest(), 16) % buckets for x in items]
    new_items = []
    for i, it in enumerate(items):
        it_new = {
            'pk': ids[i],
            'id': it.pop(id_column),
        }
        for key, val in it.items():
            it_new[key] = str(val)
        new_items.append(it_new)
    return new_items


def get_static_options(buckets=DEFAULT_BUCKETS):
    options = {
        'partition_rows': {
            'rows': ['pk'],
            'types': ['int']
        },
        'sort_row': {
            'rows': ['id'],
            'types': ['text']
        },
        'columns': {
            'info': ['all'],
        }
    }
    if buckets != DEFAULT_BUCKETS:
        options['comment'] = f"buckets={buckets}"        # nombre de buckets de la taula: no es pot canviar mai
    return options


def valid_buckets(buckets):
    return isinstance(buckets, int) and not isinstance(buckets, bool) and buckets > 0


def message_buckets(record):
    """Buckets d'un missatge: el seu `buckets`, o 100 si no en porta. Aixeca ValueError si no és un enter positiu."""
    buckets = record.get('buckets', DEFAULT_BUCKETS)
    if not valid_buckets(buckets):
        raise ValueError(f"'buckets' must be a positive integer, got {buckets!r}")
    return buckets


def check_static_buckets(session, table, buckets, verified):
    """Una taula estàtica té un nombre de buckets fix: la `pk` és md5(id) % buckets, i amb un altre nombre una fila acabaria
    en una partició diferent i quedaria duplicada. Es mira un cop per taula (`verified`) el que diu el comentari de la taula
    existent (`buckets=N`; sense comentari, el valor d'abans, 100). Retorna None si és correcte o el text del problema."""
    if table in verified:
        return None
    keyspace, name = table.split('.', 1)
    row = session.execute("SELECT comment FROM system_schema.tables WHERE keyspace_name = %s AND table_name = %s",
                          (keyspace, name)).one()
    if row is not None:
        comment = row.comment or ""
        existing = int(comment.split("buckets=", 1)[1]) if comment.startswith("buckets=") else DEFAULT_BUCKETS
        if existing != buckets:
            return f"{table} té {existing} buckets i el missatge en porta {buckets}"
    verified.add(table)
    return None


def static_buckets_problem(session, table, record, buckets, verified):
    """`check_static_buckets` per a un missatge. Si no es pot llegir el comentari de la taula (permisos, Cassandra), un
    missatge antic (sense `buckets`) es desa igualment com sempre, i s'avisa un sol cop; un amb `buckets` falla, perquè
    no es pot assegurar que la taula tingui aquest nombre."""
    try:
        return check_static_buckets(session, table, buckets, verified)
    except Exception:
        if 'buckets' in record:
            raise
        logger.warning(f"Could not read the buckets of {table}, saving with the default ({DEFAULT_BUCKETS})", exc_info=True)
        verified.add(table)
        return None


def store_consumer(database):
    logger.info(f"Starting consumer (database={database})", extra={"cloudwatch": True})
    load_dotenv.load_dotenv()
    conf = beelib.beeconfig.read_config()
    consumer = beelib.beekafka.create_kafka_consumer(conf['kafka']['connection'], encoding="JSON",
                                                     group_id=conf['kafka']['consumer_group'])
    consumer.subscribe(pattern=conf['kafka']['listen_topic'])
    # k8s envia SIGTERM en cada deploy/restart: s'acaba el missatge en curs, es commitejen els offsets i es surt del grup.
    signal.signal(signal.SIGTERM, lambda signum, frame: consumer.stop())
    signal.signal(signal.SIGINT, lambda signum, frame: consumer.stop())
    try:
        producer_ovh = beelib.beekafka.create_kafka_producer(conf['kafka_ovh']['connection'], encoding="JSON")
    except Exception as e:
        logger.warning(f"[kafka_ovh] Producer no disponible, es desactiva el reenviament: {e}", exc_info=True)
        producer_ovh = None
    if database == "cassandra":
        session, cluster = beelib.beecassandra.get_session(conf['cassandra'])
    else:
        session = None
        cluster = None
    ts_restart_ovh = None

    summary_interval_s = 300
    stats = {"ok": 0, "failed": 0, "total": 0}
    invalid_metadata = {"missing_tables": 0, "missing_row_keys": 0, "length_mismatch": 0, "invalid_buckets": 0,
                        "buckets_mismatch": 0}
    verified_buckets = set()
    save_errors_by_table = {}
    last_summary_ts = time.time()

    def flush_summary_if_due():
        nonlocal last_summary_ts
        now = time.time()
        if stats["total"] == 0 or now - last_summary_ts < summary_interval_s:
            return
        summary = {
            "window_seconds": round(now - last_summary_ts, 1),
            "records_total": stats["total"],
            "records_ok": stats["ok"],
            "records_failed": stats["failed"],
            "invalid_metadata": {k: v for k, v in invalid_metadata.items() if v},
            "save_errors_by_table": dict(save_errors_by_table),
        }
        if stats["failed"] == 0:
            logger.info(f"Consumer batch summary: {summary}", extra={"cloudwatch": True})
        elif stats["ok"] > 0:
            logger.warning(f"Consumer batch summary: {summary}")
        else:
            logger.error(f"Consumer batch summary: {summary}")
        stats["ok"] = stats["failed"] = stats["total"] = 0
        invalid_metadata.update({k: 0 for k in invalid_metadata})
        save_errors_by_table.clear()
        last_summary_ts = now

    for record in consumer:
        if ts_restart_ovh and (ts_restart_ovh + 10*60) <= time.time():
            try:
                producer_ovh = beelib.beekafka.create_kafka_producer(conf['kafka_ovh']['connection'], encoding="JSON")
                ts_restart_ovh = None
            except Exception as e:
                logger.warning(f"[kafka_ovh] Producer no disponible, es desactiva el reenviament: {e}", exc_info=True)
                producer_ovh = None
                ts_restart_ovh = time.time()
        if producer_ovh is not None:
            try:
                producer_ovh.send(record.topic, value=record.value)
                pending = producer_ovh.producer.flush(timeout=2)
                if pending > 0:
                    logger.warning(f"[kafka_ovh] flush timeout: {pending} missatges no enviats, es desactiva el reenviament")
                    producer_ovh = None
                    ts_restart_ovh = time.time()
            except Exception as e:
                logger.warning(f"[kafka_ovh] Error reenviant missatge: {e}", exc_info=True)
                producer_ovh = None
                ts_restart_ovh = time.time()
        record = record.value
        start = time.time()
        stats["total"] += 1
        if "tables" in record:
            tables = record['tables']
        else:
            invalid_metadata["missing_tables"] += 1
            stats["failed"] += 1
            logger.debug("'tables' not found in metadata")
            flush_summary_if_due()
            continue
        if "row_keys" in record:
            row_keys = record['row_keys']
        else:
            invalid_metadata["missing_row_keys"] += 1
            stats["failed"] += 1
            logger.debug("'row_keys' not found in metadata")
            flush_summary_if_due()
            continue
        if len(tables) != len(row_keys):
            invalid_metadata["length_mismatch"] += 1
            stats["failed"] += 1
            logger.debug("'tables' and 'row_keys' must be equal length")
            flush_summary_if_due()
            continue
        original_data = record['data']
        record_failed = False
        for index, table in enumerate(tables):
            row_key = row_keys[index]
            data = [dict(item) for item in original_data]
            try:
                if database == "hbase":
                    beelib.beehbase.save_to_hbase(data, table, conf['hbase']['connection'],
                                                  [("info", "all")],
                                                  row_key)
                elif database == "cassandra":
                    # TODO: REMOVE WHEN MIGRATED TO ALL INGESTORS
                    if ["device", "timestamp"] == row_key:
                        cassandra_table = table.replace(":", ".")
                        data = transform_data_ts(data, 'timestamp', 'device')
                        options = get_ts_options()
                    elif ["id", "ts"] == row_key:
                        cassandra_table = table.replace(":", ".")
                        data = transform_data_ts(data, 'ts', 'id')
                        options = get_ts_options()
                    elif ['uri', 'utcdate'] == row_key:
                        cassandra_table = table.replace(":", ".")
                        data = transform_data_ts(data, 'utcdate', 'uri')
                        options = get_ts_options()
                    elif ['uri', 'periode'] == row_key:
                        cassandra_table = table.replace(":", ".")
                        data = transform_data_ts(data, 'periode', 'uri')
                        options = get_ts_options()
                    elif ['uri', 'utcdate', 'seq'] == row_key:
                        cassandra_table = table.replace(":", ".")
                        data = transform_data_ts(data, 'utcdate', 'uri', seq_column='seq')
                        options = get_ts_options(with_seq=True)
                    elif ['uri', 'periode', 'seq'] == row_key:
                        cassandra_table = table.replace(":", ".")
                        data = transform_data_ts(data, 'periode', 'uri', seq_column='seq')
                        options = get_ts_options(with_seq=True)
                    elif ["uri", "DATA_HORA_LECTURA"] == row_key:
                        cassandra_table = table.replace(":", ".")
                        data = transform_data_ts(data, 'DATA_HORA_LECTURA', 'uri')
                        options = get_ts_options()
                    elif ['id'] == row_key:
                        cassandra_table = table.replace(":", ".")
                        try:
                            buckets = message_buckets(record)
                        except ValueError:
                            invalid_metadata["invalid_buckets"] += 1
                            logger.error(f"Invalid 'buckets' for {table}: {record.get('buckets')!r}")
                            raise
                        problem = static_buckets_problem(session, cassandra_table, record, buckets, verified_buckets)
                        if problem:
                            invalid_metadata["buckets_mismatch"] += 1
                            logger.error(f"Buckets mismatch, the message is not saved: {problem}", extra={"cloudwatch": True})
                            raise ValueError(problem)
                        data = transform_data_static(data, 'id', buckets)
                        options = get_static_options(buckets)
                    elif ["uri_measurement", "timestamp"] == row_key:
                        cassandra_table = table.replace(":", ".")
                        data = transform_data_ts(data, 'timestamp', 'uri_measurement')
                        options = get_ts_options()
                    else:
                        continue
                    beelib.beecassandra.save_to_cassandra(data, cassandra_table, session, options)
                elif database == "none":
                    continue
            except Exception as e:
                record_failed = True
                save_errors_by_table[table] = save_errors_by_table.get(table, 0) + 1
                logger.debug(f"Error saving to {table} with {e}", exc_info=True)
        stats["failed" if record_failed else "ok"] += 1
        logger.info(f"saved with processing time {time.time() - start}")
        flush_summary_if_due()

    logger.info("Shutdown signal received, closing consumer", extra={"cloudwatch": True})
    consumer.close()
    if cluster is not None:
        cluster.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", "-l", default="INFO", help="The log level")
    parser.add_argument("--database", "-db", default="hbase", choices=['hbase', 'cassandra', 'none'], help="The database to save")
    if os.getenv("PYCHARM_HOSTED") is not None:
        args = parser.parse_args(["--database", "hbase"])
    else:
        args = parser.parse_args()
        setup_logging(level=args.log)
        try:
            store_consumer(args.database)
        except Exception:
            logger.critical("Unhandled exception in raw-store consumer", exc_info=True)
            raise
