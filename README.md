# Raw Store

Kafka consumer that persists the raw time series published by the ingestors
into **HBase** or **Cassandra**. It subscribes to every topic matching the
pattern `.*.hbase` and stores each message's rows in the tables declared in
the message itself.

## Message schema

Each message (JSON-encoded) contains the target tables, row keys and the
records:

```json
{
  "tables": [<list of tables>],
  "row_keys": [<list of row_keys>],
  "data": [<list of records>]
}
```

Ingestors produce this schema with `beelib`:

```python
import beelib

data = [{"id": "1", "time": 1711128387, "value": 1, "meta_info1": "a"}]
config = beelib.beeconfig.read_config()
producer = beelib.beekafka.create_kafka_producer(config['kafka']['connection'], encoding="JSON")
beelib.beekafka.send_to_kafka(producer, "test.hbase", None, data,
                              tables=["sourceAPI:raw_sourceAPI_PT1H_"],
                              row_keys=[["id", "time"]])
```

The example above stores `value`/`meta_info1` as `info:*` columns under the
row key `id~time` in `sourceAPI:raw_sourceAPI_PT1H_`.

> When creating a new topic, wait up to 5 minutes for the consumer to detect
> it and subscribe; messages sent before the subscription are lost.

## Run

```bash
pip install -r requirements.txt
python __main__.py --database hbase        # or: --database cassandra
```

Config is read via `beelib.beeconfig.read_config()` (`CONF_FILE`); see
`test_connection.py` to check the HBase/Cassandra/Kafka connectivity.
Docker image via `Dockerfile`; production manifests in `kubeconfig/`.

## License

This project is licensed under the European Union Public Licence v. 1.2
(EUPL-1.2), see the [LICENCE](LICENCE) file.

## Contributors

- Miriam Rodriguez (mrodriguez@cimne.upc.edu)
- [@ArianH23](https://github.com/ArianH23)
- [@eloigabal](https://github.com/eloigabal)
- [@nuuuria](https://github.com/nuuuria)
- [@josemanuel97](https://github.com/josemanuel97)
