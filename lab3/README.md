# Lab 3. Structured Streaming

The script reads transactions data from provided Kafka server and topic, aggregates it, and saves results to the provided output path.
Also you need provide additional package argument to `spark-submit` command:
`--packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8`

## Input arguments
`--help` - show description

`--server`/`-s` - specofy kafka server. Default value = `localhost:9092`

`--topic`/`-t` - speficy kafka topic. Default value = `bitstamp.btc.transactions`

`--output_path`/`-o` - specify output buckert. Default value = `gs://gl-procamp-bigdata-datasets/lab3`
