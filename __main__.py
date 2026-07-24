import argparse
import time
import beelib
import load_dotenv
import logging
import os
import pandas as pd
import hashlib

from logging_setup import setup_logging

logger = logging.getLogger(__name__)


def transform_data_ts(items, ts_column, hash_column):
    years = [pd.to_datetime(x[ts_column], unit="s").year for x in items]
    new_items = []
    for i, it in enumerate(items):
        it_new = {
            'year': years[i],
            'hash': it.pop(hash_column),
            'ts': it.pop(ts_column)
        }
        for key, val in it.items():
            it_new[key] = str(val)
        new_items.append(it_new)
    return new_items


def get_ts_options():
    return {
        'partition_rows': {
            'rows': ['hash', 'year'],
            'types': ['text', 'int']
        },
        'sort_row': {
            'rows': ['ts'],
            'types': ['int']
        },
        'columns': {
            'info': ['all'],
        }
    }


def transform_data_static(items, id_column):
    ids = [int(hashlib.md5(x[id_column].encode()).hexdigest(), 16) % 100 for x in items]
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


def get_static_options():
    return {
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


def store_consumer(database):
    logger.info(f"Starting consumer (database={database})", extra={"cloudwatch": True})
    load_dotenv.load_dotenv()
    conf = beelib.beeconfig.read_config()
    consumer = beelib.beekafka.create_kafka_consumer(conf['kafka']['connection'], encoding="JSON",
                                                     group_id=conf['kafka']['consumer_group'])
    consumer.subscribe(pattern=conf['kafka']['listen_topic'])
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
    invalid_metadata = {"missing_tables": 0, "missing_row_keys": 0, "length_mismatch": 0}
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
                    elif ["uri", "DATA_HORA_LECTURA"] == row_key:
                        cassandra_table = table.replace(":", ".")
                        data = transform_data_ts(data, 'DATA_HORA_LECTURA', 'uri')
                        options = get_ts_options()
                    elif ['id'] == row_key:
                        cassandra_table = table.replace(":", ".")
                        data = transform_data_static(data, 'id')
                        options = get_static_options()
                    elif ["uri_measurement", "timestamp"] == row_key:
                        cassandra_table = table.replace(":", ".")
                        data = transform_data_ts(data, 'timestamp', 'uri_measurement')
                        options = get_ts_options()
                    else:
                        continue
                    beelib.beecassandra.save_to_cassandra(data, cassandra_table, session, options)

            except Exception as e:
                record_failed = True
                save_errors_by_table[table] = save_errors_by_table.get(table, 0) + 1
                logger.debug(f"Error saving to {table} with {e}", exc_info=True)
        stats["failed" if record_failed else "ok"] += 1
        logger.info(f"saved with processing time {time.time() - start}")
        flush_summary_if_due()


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
