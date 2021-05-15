### spark 사용의 기초

1. create sparksession
2. create spark dataframe schema

3. create sparksql metadatabase


4. test spark

```
spark  = SparkSession.builder.enableHiveSupport().getOrCreate()

    #.master("ip")

#spark.conf.set("spark.executor.memory", '1g')
#spark.conf.set('spark.executor.cores', '1')
#spark.conf.set('spark.cores.max', '1')
#spark.conf.set("spark.driver.memory",'1g')
sc = spark.sparkContext
```