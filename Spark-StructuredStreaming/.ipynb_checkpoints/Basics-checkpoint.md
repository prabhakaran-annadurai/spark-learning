## Streaming overview

1. Same optimized spark SQL engine used
2. End-to-End-Exactly-Once Fault-tolerance
3. Default - microbatches - 100 ms
4. Spark 2.3 - Continuous processing - 1 ms
5. Some operations require State(remembering from past) to be maintained in memory of spark-engine and some don't


New rows appended to unbounded table


### Modes:

|Mode|When to use |
|-|-|
|Complete| Aggregation on whole data|
|Update | Works with Aggregation, Writes only changed & new data |
|Append | Considers only new data, doesn't work with Aggregation without watermark|

### Input Sources

1. File source:

- Files processed in the order of modification time (can be reversed).
- Supported formats : csv, json, text, orc, parquet
- Can archive/ delete processed files but additional overhead

2. Kafka

3. Testing sources: Socket, Rate(timestamp, value)

