# Spark Partititon


- config 

###  partion 정보 확인


```python
# scala
df.rdd.getNumPartitions
df.rdd.partitions.length
df.rdd.partitions.size

# python
df.rdd.getNumPartitions()


```

### plan  정보 확인


```python
# scala
spark.sessionState.executePlan(df.queryExecution.logical).optimizedPlan.stats.sizeInBytes

# python
spark._jsparkSession.sessionState().executePlan(df._jdf.queryExecution().logical()).optimizedPlan().stats().sizeInBytes() 

```
