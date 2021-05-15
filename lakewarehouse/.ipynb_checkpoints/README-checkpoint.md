# spark data warehouse

# delta lake 를 이용한 warehouse 구축 

- dela lake 구성
- python 3.8 이상 
```
pip install deltalake 
```

### DW 위치를 지정하여 sparksession 구성 

```
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

 
db_path = 'd:/dw/data-warehouse/stocklab_db'

spark = SparkSession.builder.appName('test_spark') \
  .config("spark.sql.warehouse.dir", db_path) \
  .enableHiveSupport() \
  .getOrCreate()

```


## delta lake 구성 

[delta lake docs ](https://docs.delta.io/0.8.0/quick-start.html7)

- pyspark shell 실행 구성 
``` 
# pyspark
pyspark --packages io.delta:delta-core_2.12:0.8.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
# scala
bin/spark-shell --packages io.delta:delta-core_2.12:0.8.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
```


- pyspark spark session 구성 config 

> delta import 는 spark conf 가 잡혀 있는 상태에서만 가능 함. ( 순서가 sparksession 생성 후에 import)
```
spark = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

from delta.tables import *
```