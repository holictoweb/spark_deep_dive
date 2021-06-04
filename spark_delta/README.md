# Spark Delta

- delta table 
- 관리되지 않는 테이블
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


- partition table 생성

```sql
-- Create table in the metastore
CREATE TABLE events (
  date DATE,
  eventId STRING,
  eventType STRING,
  data STRING)
USING DELTA
PARTITIONED BY (date)
```

```python
DeltaTable.create(spark) \
   .tableName("event") \
   .addColumn("date", DateType()) \
   .addColumn("eventId", "STRING") \
   .addColumn("eventType", StringType()) \
   .addColumn("data", "STRING") \
   .partitionedBy("date") \
   .execute()
```

### 데이터 위치 로드

```sql
CREATE TABLE events
USING DELTA
LOCATION '/mnt/delta/events'
```


### snapshot query

```sql
SELECT * FROM events TIMESTAMP AS OF '2018-10-18T22:15:12.013Z'
SELECT * FROM delta.`/mnt/delta/events` VERSION AS OF 123

SELECT * FROM events@20190101000000000
SELECT * FROM events@v123
```

```python
df1 = spark.read.format("delta").option("timestampAsOf", timestamp_string).load("/mnt/delta/events")
df2 = spark.read.format("delta").option("versionAsOf", version).load("/mnt/delta/events")
```

- time
```python
latest_version = spark.sql("SELECT max(version) FROM (DESCRIBE HISTORY delta.`/mnt/delta/events`)").collect()
df = spark.read.format("delta").option("versionAsOf", latest_version[0][0]).load("/mnt/delta/events")
```

### retention
- 기본적으로 30일 커밋 기록 유지


### DDL

```sql
ALTER TABLE table_name ADD COLUMNS (col_name data_type [COMMENT col_comment] [FIRST|AFTER colA_name], ...)
```


