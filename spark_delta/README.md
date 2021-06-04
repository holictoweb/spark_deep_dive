# Spark Delta

- delta table 

```python
events = spark.read.json("/databricks-datasets/structured-streaming/events/")
events.write.format("delta").save("/mnt/delta/events")
spark.sql("CREATE TABLE events USING DELTA LOCATION '/mnt/delta/events/'")
```

```sql
CREATE TABLE events
USING delta
AS SELECT *
FROM json.`/data/events/`
```
