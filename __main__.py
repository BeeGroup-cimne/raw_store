import argparse
import time
import beelib
import load_dotenv
import logging
import os
import pandas as pd
import hashlib


def add_partition_key_ts(items, ts_column):
    years = [pd.to_datetime(x[ts_column], unit="s").year for x in items]
    for i, it in enumerate(items):
        it['year'] = years[i]
    return items


def add_partition_key_static(items, id_column):
    ids = [int(hashlib.md5(x[id_column].encode()).hexdigest(), 16) % 100 for x in items]
    for i, it in enumerate(items):
        it['pk'] = ids[i]
    return items



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
                        options = {
                            'partition_rows': {
                                'rows': ['device', 'year'],
                                'types': ['text', 'int']
                            },
                            'sort_row': {
                                'rows': ['timestamp'],
                                'types': ['int']
                            },
                            'columns': {
                                'info': ['all'],
                            }
                        }
                        cassandra_table = table.replace(":", ".")
                        record['data'] = add_partition_key_ts(record['data'], 'timestamp')
                    elif ["id", "ts"] == row_key:
                        options = {
                            'partition_rows': {
                                'rows': ['id', 'year'],
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
                        cassandra_table = table.replace(":", ".")
                        record['data'] = add_partition_key_ts(record['data'], 'ts')

                    elif ['uri', 'utcdate'] == row_key:
                        options = {
                            'partition_rows': {
                                'rows': ['uri', 'year'],
                                'types': ['text', 'int']
                            },
                            'sort_row': {
                                'rows': ['utcdate'],
                                'types': ['int']
                            },
                            'columns': {
                                'info': ['all'],
                            }
                        }
                        cassandra_table = table.replace(":", ".")
                        record['data'] = add_partition_key_ts(record['data'], 'utcdate')
                    elif ['uri', 'periode'] == row_key:
                        options = {
                            'partition_rows': {
                                'rows': ['uri', 'year'],
                                'types': ['text', 'int']
                            },
                            'sort_row': {
                                'rows': ['periode'],
                                'types': ['int']
                            },
                            'columns': {
                                'info': ['all'],
                            }
                        }
                        cassandra_table = table.replace(":", ".")
                        record['data'] = add_partition_key_ts(record['data'], 'periode')
                    elif ["uri", "DATA_HORA_LECTURA"] == row_key:
                        options = {
                            'partition_rows': {
                                'rows': ['uri', 'year'],
                                'types': ['text', 'int']
                            },
                            'sort_row': {
                                'rows': ['DATA_HORA_LECTURA'],
                                'types': ['int']
                            },
                            'columns': {
                                'info': ['all'],
                            }
                        }
                        cassandra_table = table.replace(":", ".")
                        record['data'] = add_partition_key_ts(record['data'], 'DATA_HORA_LECTURA')
                    elif ['id'] == row_key:
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
                        cassandra_table = table.replace(":", ".")
                        record['data'] = add_partition_key_static(record['data'], 'id')
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
