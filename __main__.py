import argparse
import time
import beelib
import load_dotenv
import logging
import os
import pandas as pd
import hashlib


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
    logger.info("Starting consumer")
    load_dotenv.load_dotenv()
    conf = beelib.beeconfig.read_config()
    consumer = beelib.beekafka.create_kafka_consumer(conf['kafka']['connection'], encoding="JSON",
                                                     group_id=conf['kafka']['consumer_group'])
    consumer.subscribe(pattern=conf['kafka']['listen_topic'])
    if database == "cassandra":
        session, cluster = beelib.beecassandra.get_session(conf['cassandra'])
    else:
        session = None
        cluster = None

    for record in consumer:
        record = record.value
        print(record['data'])
        start = time.time()
        if "tables" in record:
            tables = record['tables']
        else:
            logger.error(f"'tables' not found in metadata")
            continue
        if "row_keys" in record:
            row_keys = record['row_keys']
        else:
            logger.error(f"'row_keys' not found in metadata")
            continue
        if len(tables) != len(row_keys):
            logger.error(f"'tables' and 'row_keys' must be equal length")
            continue
        for index, table in enumerate(tables):
            row_key = row_keys[index]
            try:
                if database == "hbase":
                    beelib.beehbase.save_to_hbase(record['data'], table, conf['hbase']['connection'],
                                                  [("info", "all")],
                                                  row_key)
                elif database == "cassandra":
                    # TODO: REMOVE WHEN MIGRATED TO ALL INGESTORS
                    if ["device", "timestamp"] == row_key:
                        cassandra_table = table.replace(":", ".")
                        record['data'] = transform_data_ts(record['data'], 'timestamp', 'device')
                        options = get_ts_options()
                    elif ["id", "ts"] == row_key:
                        cassandra_table = table.replace(":", ".")
                        record['data'] = transform_data_ts(record['data'], 'ts', 'id')
                        options = get_ts_options()
                    elif ['uri', 'utcdate'] == row_key:
                        cassandra_table = table.replace(":", ".")
                        record['data'] = transform_data_ts(record['data'], 'utcdate', 'uri')
                        options = get_ts_options()
                    elif ['uri', 'periode'] == row_key:
                        cassandra_table = table.replace(":", ".")
                        record['data'] = transform_data_ts(record['data'], 'periode', 'uri')
                        options = get_ts_options()
                    elif ["uri", "DATA_HORA_LECTURA"] == row_key:
                        cassandra_table = table.replace(":", ".")
                        record['data'] = transform_data_ts(record['data'], 'DATA_HORA_LECTURA', 'uri')
                        options = get_ts_options()
                    elif ['id'] == row_key:
                        cassandra_table = table.replace(":", ".")
                        record['data'] = transform_data_static(record['data'], 'id')
                        options = get_static_options()
                    else:
                        continue
                    beelib.beecassandra.save_to_cassandra(record['data'], cassandra_table, session, options)

            except Exception as e:
                logger.error(f"Error saving {record['data']} to {table} with {e}")
        logger.info(f"saved with processing time {time.time() - start}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", "-l", default="INFO", help="The log level")
    parser.add_argument("--database", "-db", default="hbase", choices=['hbase', 'cassandra'], help="The database to save")
    if os.getenv("PYCHARM_HOSTED") is not None:
        args = parser.parse_args(["--database", "hbase"])
    else:
        args = parser.parse_args()
        logging.basicConfig(format='%(levelname)s:%(asctime)s:%(message)s', level=args.log)
        logger = logging.getLogger(__name__)
        store_consumer(args.database)
