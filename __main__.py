import argparse
import time
import beelib
import load_dotenv
import logging


def store_consumer():
    logger.info("Starting consumer")
    load_dotenv.load_dotenv()
    conf = beelib.beeconfig.read_config()
    consumer = beelib.beekafka.create_kafka_consumer(conf['kafka'], encoding="JSON", group_id="store_raw_data")
    consumer.subscribe(pattern=".*.hbase")
    for record in consumer:
        record = record.value
        start = time.time()
        if "table" in record:
            hbase_table = record['table']
        else:
            logger.error(f"'table' not found in metadata")
            continue
        if "row_keys" in record:
            row_keys = record['row_keys']
        else:
            logger.error(f"'row_keys' not found in metadata")
            continue
        try:
            beelib.beehbase.save_to_hbase(record['data'], hbase_table, conf['hbase']['connection'], [("info", "all")],
                                          row_keys)
        except Exception as e:
            logger.error(f"Error saving {record['data']} to {hbase_table} with {e}")
        logger.info(f"saved with processing time {time.time() - start}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", "-l", default="INFO", help="The log level")
    args = parser.parse_args()
    logging.basicConfig(format='%(levelname)s:%(asctime)s:%(message)s', level=args.log)
    logger = logging.getLogger(__name__)
    store_consumer()
