### spark conf

https://spark.apache.org/docs/latest/configuration.html

1. runtime
2. shuffle


# config 설정 
### SparkConf를 통해 sparksession 생성 시 설정 
```
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import lit



db_path = 'd:/dw/data-warehouse/stocklab_db'
appName = "test_spark"
master = "local[4]"


conf = SparkConf().setAll([("spark.driver.maxResultSize", '6g'), ('spark.executor.cores', '1'), ('spark.submit.deployMode', 'cluster')])

spark = SparkSession.builder.appName(appName) \
  .master(master) \
  .config(conf=conf) \
  .config("spark.sql.warehouse.dir", db_path) \
  .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
  .enableHiveSupport() \
  .getOrCreate()

from delta.tables import *
from pyspark.sql.functions import *
```

- conf 확인
```
sc = spark.sparkContext

sc.getConf().getAll()
```




### Runtime Environment

| Property Name                                   | Default                     | Meaning                                                      | Since Version |
| :---------------------------------------------- | :-------------------------- | :----------------------------------------------------------- | :------------ |
| `spark.driver.extraClassPath`                   | (none)                      | Extra classpath entries to prepend to the classpath of the driver. *Note:* In client mode, this config must not be set through the `SparkConf` directly in your application, because the driver JVM has already started at that point. Instead, please set this through the `--driver-class-path` command line option or in your default properties file. | 1.0.0         |
| `spark.driver.defaultJavaOptions`               | (none)                      | A string of default JVM options to prepend to `spark.driver.extraJavaOptions`. This is intended to be set by administrators. For instance, GC settings or other logging. Note that it is illegal to set maximum heap size (-Xmx) settings with this option. Maximum heap size settings can be set with `spark.driver.memory` in the cluster mode and through the `--driver-memory` command line option in the client mode. *Note:* In client mode, this config must not be set through the `SparkConf` directly in your application, because the driver JVM has already started at that point. Instead, please set this through the `--driver-java-options` command line option or in your default properties file. | 3.0.0         |
| `spark.driver.extraJavaOptions`                 | (none)                      | A string of extra JVM options to pass to the driver. This is intended to be set by users. For instance, GC settings or other logging. Note that it is illegal to set maximum heap size (-Xmx) settings with this option. Maximum heap size settings can be set with `spark.driver.memory` in the cluster mode and through the `--driver-memory` command line option in the client mode. *Note:* In client mode, this config must not be set through the `SparkConf` directly in your application, because the driver JVM has already started at that point. Instead, please set this through the `--driver-java-options` command line option or in your default properties file. `spark.driver.defaultJavaOptions` will be prepended to this configuration. | 1.0.0         |
| `spark.driver.extraLibraryPath`                 | (none)                      | Set a special library path to use when launching the driver JVM. *Note:* In client mode, this config must not be set through the `SparkConf` directly in your application, because the driver JVM has already started at that point. Instead, please set this through the `--driver-library-path` command line option or in your default properties file. | 1.0.0         |
| `spark.driver.userClassPathFirst`               | false                       | (Experimental) Whether to give user-added jars precedence over Spark's own jars when loading classes in the driver. This feature can be used to mitigate conflicts between Spark's dependencies and user dependencies. It is currently an experimental feature. This is used in cluster mode only. | 1.3.0         |
| `spark.executor.extraClassPath`                 | (none)                      | Extra classpath entries to prepend to the classpath of executors. This exists primarily for backwards-compatibility with older versions of Spark. Users typically should not need to set this option. | 1.0.0         |
| `spark.executor.defaultJavaOptions`             | (none)                      | A string of default JVM options to prepend to `spark.executor.extraJavaOptions`. This is intended to be set by administrators. For instance, GC settings or other logging. Note that it is illegal to set Spark properties or maximum heap size (-Xmx) settings with this option. Spark properties should be set using a SparkConf object or the spark-defaults.conf file used with the spark-submit script. Maximum heap size settings can be set with spark.executor.memory. The following symbols, if present will be interpolated: will be replaced by application ID and will be replaced by executor ID. For example, to enable verbose gc logging to a file named for the executor ID of the app in /tmp, pass a 'value' of: `-verbose:gc -Xloggc:/tmp/-.gc` | 3.0.0         |
| `spark.executor.extraJavaOptions`               | (none)                      | A string of extra JVM options to pass to executors. This is intended to be set by users. For instance, GC settings or other logging. Note that it is illegal to set Spark properties or maximum heap size (-Xmx) settings with this option. Spark properties should be set using a SparkConf object or the spark-defaults.conf file used with the spark-submit script. Maximum heap size settings can be set with spark.executor.memory. The following symbols, if present will be interpolated: will be replaced by application ID and will be replaced by executor ID. For example, to enable verbose gc logging to a file named for the executor ID of the app in /tmp, pass a 'value' of: `-verbose:gc -Xloggc:/tmp/-.gc` `spark.executor.defaultJavaOptions` will be prepended to this configuration. | 1.0.0         |
| `spark.executor.extraLibraryPath`               | (none)                      | Set a special library path to use when launching executor JVM's. | 1.0.0         |
| `spark.executor.logs.rolling.maxRetainedFiles`  | (none)                      | Sets the number of latest rolling log files that are going to be retained by the system. Older log files will be deleted. Disabled by default. | 1.1.0         |
| `spark.executor.logs.rolling.enableCompression` | false                       | Enable executor log compression. If it is enabled, the rolled executor logs will be compressed. Disabled by default. | 2.0.2         |
| `spark.executor.logs.rolling.maxSize`           | (none)                      | Set the max size of the file in bytes by which the executor logs will be rolled over. Rolling is disabled by default. See `spark.executor.logs.rolling.maxRetainedFiles` for automatic cleaning of old logs. | 1.4.0         |
| `spark.executor.logs.rolling.strategy`          | (none)                      | Set the strategy of rolling of executor logs. By default it is disabled. It can be set to "time" (time-based rolling) or "size" (size-based rolling). For "time", use `spark.executor.logs.rolling.time.interval` to set the rolling interval. For "size", use `spark.executor.logs.rolling.maxSize` to set the maximum file size for rolling. | 1.1.0         |
| `spark.executor.logs.rolling.time.interval`     | daily                       | Set the time interval by which the executor logs will be rolled over. Rolling is disabled by default. Valid values are `daily`, `hourly`, `minutely` or any interval in seconds. See `spark.executor.logs.rolling.maxRetainedFiles` for automatic cleaning of old logs. | 1.1.0         |
| `spark.executor.userClassPathFirst`             | false                       | (Experimental) Same functionality as `spark.driver.userClassPathFirst`, but applied to executor instances. | 1.3.0         |
| `spark.executorEnv.[EnvironmentVariableName]`   | (none)                      | Add the environment variable specified by `EnvironmentVariableName` to the Executor process. The user can specify multiple of these to set multiple environment variables. | 0.9.0         |
| `spark.redaction.regex`                         | (?i)secret\|password\|token | Regex to decide which Spark configuration properties and environment variables in driver and executor environments contain sensitive information. When this regex matches a property key or value, the value is redacted from the environment UI and various logs like YARN and event logs. | 2.1.2         |
| `spark.python.profile`                          | false                       | Enable profiling in Python worker, the profile result will show up by `sc.show_profiles()`, or it will be displayed before the driver exits. It also can be dumped into disk by `sc.dump_profiles(path)`. If some of the profile results had been displayed manually, they will not be displayed automatically before driver exiting. By default the `pyspark.profiler.BasicProfiler` will be used, but this can be overridden by passing a profiler class in as a parameter to the `SparkContext` constructor. | 1.2.0         |
| `spark.python.profile.dump`                     | (none)                      | The directory which is used to dump the profile result before driver exiting. The results will be dumped as separated file for each RDD. They can be loaded by `pstats.Stats()`. If this is specified, the profile result will not be displayed automatically. | 1.2.0         |
| `spark.python.worker.memory`                    | 512m                        | Amount of memory to use per python worker process during aggregation, in the same format as JVM memory strings with a size unit suffix ("k", "m", "g" or "t") (e.g. `512m`, `2g`). If the memory used during aggregation goes above this amount, it will spill the data into disks. | 1.1.0         |
| `spark.python.worker.reuse`                     | true                        | Reuse Python worker or not. If yes, it will use a fixed number of Python workers, does not need to fork() a Python process for every task. It will be very useful if there is a large broadcast, then the broadcast will not need to be transferred from JVM to Python worker for every task. | 1.2.0         |
| `spark.files`                                   |                             | Comma-separated list of files to be placed in the working directory of each executor. Globs are allowed. | 1.0.0         |
| `spark.submit.pyFiles`                          |                             | Comma-separated list of .zip, .egg, or .py files to place on the PYTHONPATH for Python apps. Globs are allowed. | 1.0.1         |
| `spark.jars`                                    |                             | Comma-separated list of jars to include on the driver and executor classpaths. Globs are allowed. | 0.9.0         |
| `spark.jars.packages`                           |                             | Comma-separated list of Maven coordinates of jars to include on the driver and executor classpaths. The coordinates should be groupId:artifactId:version. If `spark.jars.ivySettings` is given artifacts will be resolved according to the configuration in the file, otherwise artifacts will be searched for in the local maven repo, then maven central and finally any additional remote repositories given by the command-line option `--repositories`. For more details, see [Advanced Dependency Management](https://spark.apache.org/docs/latest/submitting-applications.html#advanced-dependency-management). | 1.5.0         |
| `spark.jars.excludes`                           |                             | Comma-separated list of groupId:artifactId, to exclude while resolving the dependencies provided in `spark.jars.packages` to avoid dependency conflicts. | 1.5.0         |
| `spark.jars.ivy`                                |                             | Path to specify the Ivy user directory, used for the local Ivy cache and package files from `spark.jars.packages`. This will override the Ivy property `ivy.default.ivy.user.dir` which defaults to ~/.ivy2. | 1.3.0         |
| `spark.jars.ivySettings`                        |                             | Path to an Ivy settings file to customize resolution of jars specified using `spark.jars.packages` instead of the built-in defaults, such as maven central. Additional repositories given by the command-line option `--repositories` or `spark.jars.repositories` will also be included. Useful for allowing Spark to resolve artifacts from behind a firewall e.g. via an in-house artifact server like Artifactory. Details on the settings file format can be found at [Settings Files](http://ant.apache.org/ivy/history/latest-milestone/settings.html) | 2.2.0         |
| `spark.jars.repositories`                       |                             | Comma-separated list of additional remote repositories to search for the maven coordinates given with `--packages` or `spark.jars.packages`. | 2.3.0         |
| `spark.archives`                                |                             | Comma-separated list of archives to be extracted into the working directory of each executor. .jar, .tar.gz, .tgz and .zip are supported. You can specify the directory name to unpack via adding `#` after the file name to unpack, for example, `file.zip#directory`. This configuration is experimental. | 3.1.0         |
| `spark.pyspark.driver.python`                   |                             | Python binary executable to use for PySpark in driver. (default is `spark.pyspark.python`) | 2.1.0         |
| `spark.pyspark.python`                          |                             | Python binary executable to use for PySpark in both driver and executors. | 2.1.0         |







### Shuffle Behavior

| Property Name                               | Default           | Meaning                                                      | Since Version |
| :------------------------------------------ | :---------------- | :----------------------------------------------------------- | :------------ |
| `spark.reducer.maxSizeInFlight`             | 48m               | Maximum size of map outputs to fetch simultaneously from each reduce task, in MiB unless otherwise specified. Since each output requires us to create a buffer to receive it, this represents a fixed memory overhead per reduce task, so keep it small unless you have a large amount of memory. | 1.4.0         |
| `spark.reducer.maxReqsInFlight`             | Int.MaxValue      | This configuration limits the number of remote requests to fetch blocks at any given point. When the number of hosts in the cluster increase, it might lead to very large number of inbound connections to one or more nodes, causing the workers to fail under load. By allowing it to limit the number of fetch requests, this scenario can be mitigated. | 2.0.0         |
| `spark.reducer.maxBlocksInFlightPerAddress` | Int.MaxValue      | This configuration limits the number of remote blocks being fetched per reduce task from a given host port. When a large number of blocks are being requested from a given address in a single fetch or simultaneously, this could crash the serving executor or Node Manager. This is especially useful to reduce the load on the Node Manager when external shuffle is enabled. You can mitigate this issue by setting it to a lower value. | 2.2.1         |
| `spark.shuffle.compress`                    | true              | Whether to compress map output files. Generally a good idea. Compression will use `spark.io.compression.codec`. | 0.6.0         |
| `spark.shuffle.file.buffer`                 | 32k               | Size of the in-memory buffer for each shuffle file output stream, in KiB unless otherwise specified. These buffers reduce the number of disk seeks and system calls made in creating intermediate shuffle files. | 1.4.0         |
| `spark.shuffle.io.maxRetries`               | 3                 | (Netty only) Fetches that fail due to IO-related exceptions are automatically retried if this is set to a non-zero value. This retry logic helps stabilize large shuffles in the face of long GC pauses or transient network connectivity issues. | 1.2.0         |
| `spark.shuffle.io.numConnectionsPerPeer`    | 1                 | (Netty only) Connections between hosts are reused in order to reduce connection buildup for large clusters. For clusters with many hard disks and few hosts, this may result in insufficient concurrency to saturate all disks, and so users may consider increasing this value. | 1.2.1         |
| `spark.shuffle.io.preferDirectBufs`         | true              | (Netty only) Off-heap buffers are used to reduce garbage collection during shuffle and cache block transfer. For environments where off-heap memory is tightly limited, users may wish to turn this off to force all allocations from Netty to be on-heap. | 1.2.0         |
| `spark.shuffle.io.retryWait`                | 5s                | (Netty only) How long to wait between retries of fetches. The maximum delay caused by retrying is 15 seconds by default, calculated as `maxRetries * retryWait`. | 1.2.1         |
| `spark.shuffle.io.backLog`                  | -1                | Length of the accept queue for the shuffle service. For large applications, this value may need to be increased, so that incoming connections are not dropped if the service cannot keep up with a large number of connections arriving in a short period of time. This needs to be configured wherever the shuffle service itself is running, which may be outside of the application (see `spark.shuffle.service.enabled` option below). If set below 1, will fallback to OS default defined by Netty's `io.netty.util.NetUtil#SOMAXCONN`. | 1.1.1         |
| `spark.shuffle.service.enabled`             | false             | Enables the external shuffle service. This service preserves the shuffle files written by executors so the executors can be safely removed. The external shuffle service must be set up in order to enable it. See [dynamic allocation configuration and setup documentation](https://spark.apache.org/docs/latest/job-scheduling.html#configuration-and-setup) for more information. | 1.2.0         |
| `spark.shuffle.service.port`                | 7337              | Port on which the external shuffle service will run.         | 1.2.0         |
| `spark.shuffle.service.index.cache.size`    | 100m              | Cache entries limited to the specified memory footprint, in bytes unless otherwise specified. | 2.3.0         |
| `spark.shuffle.maxChunksBeingTransferred`   | Long.MAX_VALUE    | The max number of chunks allowed to be transferred at the same time on shuffle service. Note that new incoming connections will be closed when the max number is hit. The client will retry according to the shuffle retry configs (see `spark.shuffle.io.maxRetries` and `spark.shuffle.io.retryWait`), if those limits are reached the task will fail with fetch failure. | 2.3.0         |
| `spark.shuffle.sort.bypassMergeThreshold`   | 200               | (Advanced) In the sort-based shuffle manager, avoid merge-sorting data if there is no map-side aggregation and there are at most this many reduce partitions. | 1.1.1         |
| `spark.shuffle.spill.compress`              | true              | Whether to compress data spilled during shuffles. Compression will use `spark.io.compression.codec`. | 0.9.0         |
| `spark.shuffle.accurateBlockThreshold`      | 100 * 1024 * 1024 | Threshold in bytes above which the size of shuffle blocks in HighlyCompressedMapStatus is accurately recorded. This helps to prevent OOM by avoiding underestimating shuffle block size when fetch shuffle blocks. | 2.2.1         |
| `spark.shuffle.registration.timeout`        | 5000              | Timeout in milliseconds for registration to the external shuffle service. | 2.3.0         |
| `spark.shuffle.registration.maxAttempts`    | 3                 | When we fail to register to the external shuffle service, we will retry for maxAttempts times. | 2.3.0         |