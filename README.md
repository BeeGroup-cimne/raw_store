# STORE RAW
This application is used to store the raw data to HBase. 

It will accept messages sent to the topic partitions following the pattern `.*.hbase`

### Message Schema

The message schema that this application will accept is as follows:

```json 
{
  "tables": [<list of tables>],
  "row_keys": [<list of row_keys>],
  "data": [<list of records>],
}
```

this schema can be created by using the `beelib` module as follows

```python
import beelib

data = [{"id": "1", "time": 1711128387, "value": 1, "meta_info1": "a"}, 
        {"id": "1", "time": 1711128388, "value": 12, "meta_info1": "b"}, 
        {"id": "2", "time": 1711128387, "value": 3, "meta_info1": "a"}
]
config = beelib.beeconfig.read_config()
producer = beelib.beekafka.create_kafka_producer(config['kafka'], encoding="JSON")
# send tables and row_keys as array to allow multiple stores with the same data
beelib.beekafka.send_to_kafka(producer, "test.hbase", None, data, tables=["sourceAPI:raw_sourceAPI_PT1H_"],
                              row_keys=[["id", "time"]]) 
```
**TIPS:** 
>
>*When creating a new topic first, we must wait up to 5 minutes for the consumer to detect the new topic and 
subscribe to it.*

>*All messages sent before the subscription will be lost. We recommend to create the topic and wait for 
the consumer to subscribe*

>*Notice the messages should be sent with the JSON encoding*

>*Create the druid topic if you want to use it*


The previous example will create the key as `id~time` and the columns `info:<field>` and store it in the table `sourceAPI:raw_sourceAPI_PT1H_`

```cmd
> scan "sourceAPI:raw_sourceAPI_PT1H_"
row
1~1711128387 info:value=1
1~1711128387 info:meta_info1=a
1~1711128388 info:value=12
1~1711128388 info:meta_info1=b
2~1711128387 info:value=3
2~1711128387 info:meta_info1=a
```
