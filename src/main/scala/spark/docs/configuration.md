---
layout: global
title: Spark Configuration
---
* This will become a table of contents (this text will be scraped).
{:toc}

Spark provides three locations to configure the system:

* [Spark properties](#spark-properties) control most application parameters and can be set by using
  a [SparkConf](api/core/index.html#org.apache.spark.SparkConf) object, or through Java
  system properties.
* [Environment variables](#environment-variables) can be used to set per-machine settings, such as
  the IP address, through the `conf/spark-env.sh` script on each node.
* [Logging](#configuring-logging) can be configured through `log4j.properties`.

# Spark Properties

Spark properties control most application settings and are configured separately for each
application. These properties can be set directly on a
[SparkConf](api/scala/index.html#org.apache.spark.SparkConf) passed to your
`SparkContext`. `SparkConf` allows you to configure some of the common properties
(e.g. master URL and application name), as well as arbitrary key-value pairs through the
`set()` method. For example, we could initialize an application as follows:

{% highlight scala %}
val conf = new SparkConf()
             .setMaster("local")
             .setAppName("CountingSheep")
             .set("spark.executor.memory", "1g")
val sc = new SparkContext(conf)
{% endhighlight %}

## Dynamically Loading Spark Properties
In some cases, you may want to avoid hard-coding certain configurations in a `SparkConf`. For
instance, if you'd like to run the same application with different masters or different
amounts of memory. Spark allows you to simply create an empty conf:

{% highlight scala %}
val sc = new SparkContext(new SparkConf())
{% endhighlight %}

Then, you can supply configuration values at runtime:
{% highlight bash %}
./bin/spark-submit --name "My fancy app" --master local[4] myApp.jar
{% endhighlight %}

The Spark shell and [`spark-submit`](cluster-overview.html#launching-applications-with-spark-submit)
tool support two ways to load configurations dynamically. The first are command line options,
such as `--master`, as shown above. Running `./bin/spark-submit --help` will show the entire list
of options.

`bin/spark-submit` will also read configuration options from `conf/spark-defaults.conf`, in which
each line consists of a key and a value separated by whitespace. For example:

    spark.master            spark://5.6.7.8:7077
    spark.executor.memory   512m
    spark.eventLog.enabled  true
    spark.serializer        org.apache.spark.serializer.KryoSerializer

Any values specified as flags or in the properties file will be passed on to the application
and merged with those specified through SparkConf. Properties set directly on the SparkConf
take highest precedence, then flags passed to `spark-submit` or `spark-shell`, then options
in the `spark-defaults.conf` file.

## Viewing Spark Properties

The application web UI at `http://<driver>:4040` lists Spark properties in the "Environment" tab.
This is a useful place to check to make sure that your properties have been set correctly. Note
that only values explicitly specified through either `spark-defaults.conf` or SparkConf will
appear. For all other configuration properties, you can assume the default value is used.

## Available Properties

Most of the properties that control internal settings have reasonable default values. Some
of the most common options to set are:

#### Application Properties
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.app.name</code></td>
  <td>(none)</td>
  <td>
    The name of your application. This will appear in the UI and in log data.
  </td>
</tr>
<tr>
  <td><code>spark.master</code></td>
  <td>(none)</td>
  <td>
    The cluster manager to connect to. See the list of
    <a href="scala-programming-guide.html#master-urls"> allowed master URL's</a>.
  </td>
</tr>
<tr>
  <td><code>spark.executor.memory</code></td>
  <td>512m</td>
  <td>
    Amount of memory to use per executor process, in the same format as JVM memory strings
    (e.g. <code>512m</code>, <code>2g</code>).
  </td>
</tr>
<tr>
  <td><code>spark.serializer</code></td>
  <td>org.apache.spark.serializer.<br />JavaSerializer</td>
  <td>
    Class to use for serializing objects that will be sent over the network or need to be cached
    in serialized form. The default of Java serialization works with any Serializable Java object
    but is quite slow, so we recommend <a href="tuning.html">using
    <code>org.apache.spark.serializer.KryoSerializer</code> and configuring Kryo serialization</a>
    when speed is necessary. Can be any subclass of
    <a href="api/scala/index.html#org.apache.spark.serializer.Serializer">
    <code>org.apache.spark.Serializer</code></a>.
  </td>
</tr>
<tr>
  <td><code>spark.kryo.registrator</code></td>
  <td>(none)</td>
  <td>
    If you use Kryo serialization, set this class to register your custom classes with Kryo.
    It should be set to a class that extends
    <a href="api/scala/index.html#org.apache.spark.serializer.KryoRegistrator">
    <code>KryoRegistrator</code></a>.
    See the <a href="tuning.html#data-serialization">tuning guide</a> for more details.
  </td>
</tr>
<tr>
  <td><code>spark.local.dir</code></td>
  <td>/tmp</td>
  <td>
    Directory to use for "scratch" space in Spark, including map output files and RDDs that get
    stored on disk. This should be on a fast, local disk in your system. It can also be a
    comma-separated list of multiple directories on different disks.

    NOTE: In Spark 1.0 and later this will be overriden by SPARK_LOCAL_DIRS (Standalone, Mesos) or
    LOCAL_DIRS (YARN) environment variables set by the cluster manager.
  </td>
</tr>
<tr>
  <td><code>spark.logConf</code></td>
  <td>false</td>
  <td>
    Logs the effective SparkConf as INFO when a SparkContext is started.
  </td>
</tr>
</table>

Apart from these, the following properties are also available, and may be useful in some situations:

#### Runtime Environment
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.executor.memory</code></td>
  <td>512m</td>
  <td>
    Amount of memory to use per executor process, in the same format as JVM memory strings
    (e.g. <code>512m</code>, <code>2g</code>).
  </td>
</tr>
<tr>
  <td><code>spark.executor.extraJavaOptions</code></td>
  <td>(none)</td>
  <td>
    A string of extra JVM options to pass to executors. For instance, GC settings or other
    logging. Note that it is illegal to set Spark properties or heap size settings with this
    option. Spark properties should be set using a SparkConf object or the
    spark-defaults.conf file used with the spark-submit script. Heap size settings can be set
    with spark.executor.memory.
  </td>
</tr>
<tr>
  <td><code>spark.executor.extraClassPath</code></td>
  <td>(none)</td>
  <td>
    Extra classpath entries to append to the classpath of executors. This exists primarily
    for backwards-compatibility with older versions of Spark. Users typically should not need
    to set this option.
  </td>
</tr>
<tr>
  <td><code>spark.executor.extraLibraryPath</code></td>
  <td>(none)</td>
  <td>
    Set a special library path to use when launching executor JVM's.
  </td>
</tr>
<tr>
  <td><code>spark.files.userClassPathFirst</code></td>
  <td>false</td>
  <td>
    (Experimental) Whether to give user-added jars precedence over Spark's own jars when
    loading classes in Executors. This feature can be used to mitigate conflicts between
    Spark's dependencies and user dependencies. It is currently an experimental feature.
  </td>
</tr>
</table>

#### Shuffle Behavior
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.shuffle.consolidateFiles</code></td>
  <td>false</td>
  <td>
    If set to "true", consolidates intermediate files created during a shuffle. Creating fewer
    files can improve filesystem performance for shuffles with large numbers of reduce tasks. It
    is recommended to set this to "true" when using ext4 or xfs filesystems. On ext3, this option
    might degrade performance on machines with many (>8) cores due to filesystem limitations.
  </td>
</tr>
<tr>
  <td><code>spark.shuffle.spill</code></td>
  <td>true</td>
  <td>
    If set to "true", limits the amount of memory used during reduces by spilling data out to disk.
    This spilling threshold is specified by <code>spark.shuffle.memoryFraction</code>.
  </td>
</tr>
<tr>
  <td><code>spark.shuffle.spill.compress</code></td>
  <td>true</td>
  <td>
    Whether to compress data spilled during shuffles. Compression will use
    <code>spark.io.compression.codec</code>.
  </td>
</tr>
<tr>
  <td><code>spark.shuffle.memoryFraction</code></td>
  <td>0.3</td>
  <td>
    Fraction of Java heap to use for aggregation and cogroups during shuffles, if
    <code>spark.shuffle.spill</code> is true. At any given time, the collective size of
    all in-memory maps used for shuffles is bounded by this limit, beyond which the contents will
    begin to spill to disk. If spills are often, consider increasing this value at the expense of
    <code>spark.storage.memoryFraction</code>.
  </td>
</tr>
<tr>
  <td><code>spark.shuffle.compress</code></td>
  <td>true</td>
  <td>
    Whether to compress map output files. Generally a good idea. Compression will use
    <code>spark.io.compression.codec</code>.
  </td>
</tr>
<tr>
  <td><code>spark.shuffle.file.buffer.kb</code></td>
  <td>100</td>
  <td>
    Size of the in-memory buffer for each shuffle file output stream, in kilobytes. These buffers
    reduce the number of disk seeks and system calls made in creating intermediate shuffle files.
  </td>
</tr>
<tr>
  <td><code>spark.reducer.maxMbInFlight</code></td>
  <td>48</td>
  <td>
    Maximum size (in megabytes) of map outputs to fetch simultaneously from each reduce task. Since
    each output requires us to create a buffer to receive it, this represents a fixed memory
    overhead per reduce task, so keep it small unless you have a large amount of memory.
  </td>
</tr>
</table>

#### Spark UI
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.ui.port</code></td>
  <td>4040</td>
  <td>
    Port for your application's dashboard, which shows memory and workload data
  </td>
</tr>
<tr>
  <td><code>spark.ui.retainedStages</code></td>
  <td>1000</td>
  <td>
    How many stages the Spark UI remembers before garbage collecting.
  </td>
</tr>
<tr>
  <td><code>spark.ui.killEnabled</code></td>
  <td>true</td>
  <td>
    Allows stages and corresponding jobs to be killed from the web ui.
  </td>
</tr>
<tr>
  <td><code>spark.eventLog.enabled</code></td>
  <td>false</td>
  <td>
    Whether to log Spark events, useful for reconstructing the Web UI after the application has
    finished.
  </td>
</tr>
<tr>
  <td><code>spark.eventLog.compress</code></td>
  <td>false</td>
  <td>
    Whether to compress logged events, if <code>spark.eventLog.enabled</code> is true.
  </td>
</tr>
<tr>
  <td><code>spark.eventLog.dir</code></td>
  <td>file:///tmp/spark-events</td>
  <td>
    Base directory in which Spark events are logged, if <code>spark.eventLog.enabled</code> is true.
    Within this base directory, Spark creates a sub-directory for each application, and logs the
    events specific to the application in this directory. Users may want to set this to
    a unified location like an HDFS directory so history files can be read by the history server.
  </td>
</tr>
</table>

#### Compression and Serialization
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.broadcast.compress</code></td>
  <td>true</td>
  <td>
    Whether to compress broadcast variables before sending them. Generally a good idea.
  </td>
</tr>
<tr>
  <td><code>spark.rdd.compress</code></td>
  <td>false</td>
  <td>
    Whether to compress serialized RDD partitions (e.g. for
    <code>StorageLevel.MEMORY_ONLY_SER</code>). Can save substantial space at the cost of some
    extra CPU time.
  </td>
</tr>
<tr>
  <td><code>spark.io.compression.codec</code></td>
  <td>org.apache.spark.io.<br />LZFCompressionCodec</td>
  <td>
    The codec used to compress internal data such as RDD partitions and shuffle outputs.
    By default, Spark provides two codecs: <code>org.apache.spark.io.LZFCompressionCodec</code>
    and <code>org.apache.spark.io.SnappyCompressionCodec</code>. Of these two choices,
    Snappy offers faster compression and decompression, while LZF offers a better compression
    ratio.
  </td>
</tr>
<tr>
  <td><code>spark.io.compression.snappy.block.size</code></td>
  <td>32768</td>
  <td>
    Block size (in bytes) used in Snappy compression, in the case when Snappy compression codec
    is used.
  </td>
</tr>
<tr>
  <td><code>spark.closure.serializer</code></td>
  <td>org.apache.spark.serializer.<br />JavaSerializer</td>
  <td>
    Serializer class to use for closures. Currently only the Java serializer is supported.
  </td>
</tr>
<tr>
  <td><code>spark.serializer.objectStreamReset</code></td>
  <td>10000</td>
  <td>
    When serializing using org.apache.spark.serializer.JavaSerializer, the serializer caches
    objects to prevent writing redundant data, however that stops garbage collection of those
    objects. By calling 'reset' you flush that info from the serializer, and allow old
    objects to be collected. To turn off this periodic reset set it to a value &lt;= 0.
    By default it will reset the serializer every 10,000 objects.
  </td>
</tr>
<tr>
  <td><code>spark.kryo.referenceTracking</code></td>
  <td>true</td>
  <td>
    Whether to track references to the same object when serializing data with Kryo, which is
    necessary if your object graphs have loops and useful for efficiency if they contain multiple
    copies of the same object. Can be disabled to improve performance if you know this is not the
    case.
  </td>
</tr>
<tr>
  <td><code>spark.kryoserializer.buffer.mb</code></td>
  <td>2</td>
  <td>
    Maximum object size to allow within Kryo (the library needs to create a buffer at least as
    large as the largest single object you'll serialize). Increase this if you get a "buffer limit
    exceeded" exception inside Kryo. Note that there will be one buffer <i>per core</i> on each
    worker.
  </td>
</tr>
</table>

#### Execution Behavior
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.default.parallelism</code></td>
  <td>
    <ul>
      <li>Local mode: number of cores on the local machine</li>
      <li>Mesos fine grained mode: 8</li>
      <li>Others: total number of cores on all executor nodes or 2, whichever is larger</li>
    </ul>
  </td>
  <td>
    Default number of tasks to use across the cluster for distributed shuffle operations
    (<code>groupByKey</code>, <code>reduceByKey</code>, etc) when not set by user.
  </td>
</tr>
<tr>
  <td><code>spark.broadcast.factory</code></td>
  <td>org.apache.spark.broadcast.<br />HttpBroadcastFactory</td>
  <td>
    Which broadcast implementation to use.
  </td>
</tr>
<tr>
  <td><code>spark.broadcast.blockSize</code></td>
  <td>4096</td>
  <td>
    Size of each piece of a block in kilobytes for <code>TorrentBroadcastFactory</code>.
    Too large a value decreases parallelism during broadcast (makes it slower); however, if it is
    too small, <code>BlockManager</code> might take a performance hit.
  </td>
</tr>
<tr>
  <td><code>spark.files.overwrite</code></td>
  <td>false</td>
  <td>
    Whether to overwrite files added through SparkContext.addFile() when the target file exists and
    its contents do not match those of the source.
  </td>
</tr>
<tr>
  <td><code>spark.files.fetchTimeout</code></td>
  <td>false</td>
  <td>
    Communication timeout to use when fetching files added through SparkContext.addFile() from
    the driver.
  </td>
</tr>
<tr>
  <td><code>spark.storage.memoryFraction</code></td>
  <td>0.6</td>
  <td>
    Fraction of Java heap to use for Spark's memory cache. This should not be larger than the "old"
    generation of objects in the JVM, which by default is given 0.6 of the heap, but you can
    increase it if you configure your own old generation size.
  </td>
</tr>
<tr>
  <td><code>spark.tachyonStore.baseDir</code></td>
  <td>System.getProperty("java.io.tmpdir")</td>
  <td>
    Directories of the Tachyon File System that store RDDs. The Tachyon file system's URL is set by
    <code>spark.tachyonStore.url</code>. It can also be a comma-separated list of multiple
    directories on Tachyon file system.
  </td>
</tr>
<tr>
  <td><code>spark.storage.memoryMapThreshold</code></td>
  <td>8192</td>
  <td>
    Size of a block, in bytes, above which Spark memory maps when reading a block from disk.
    This prevents Spark from memory mapping very small blocks. In general, memory
    mapping has high overhead for blocks close to or below the page size of the operating system.
  </td>
</tr>
<tr>
  <td><code>spark.tachyonStore.url</code></td>
  <td>tachyon://localhost:19998</td>
  <td>
    The URL of the underlying Tachyon file system in the TachyonStore.
  </td>
</tr>
<tr>
  <td><code>spark.cleaner.ttl</code></td>
  <td>(infinite)</td>
  <td>
    Duration (seconds) of how long Spark will remember any metadata (stages generated, tasks
    generated, etc.). Periodic cleanups will ensure that metadata older than this duration will be
    forgotten. This is useful for running Spark for many hours / days (for example, running 24/7 in
    case of Spark Streaming applications). Note that any RDD that persists in memory for more than
    this duration will be cleared as well.
  </td>
</tr>
<tr>
    <td>spark.hadoop.validateOutputSpecs</td>
    <td>true</td>
    <td>If set to true, validates the output specification (e.g. checking if the output directory already exists) 
    used in saveAsHadoopFile and other variants. This can be disabled to silence exceptions due to pre-existing 
    output directories. We recommend that users do not disable this except if trying to achieve compatibility with 
    previous versions of Spark. Simply use Hadoop's FileSystem API to delete output directories by hand.</td>
</tr>
</table>

#### Networking
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.driver.host</code></td>
  <td>(local hostname)</td>
  <td>
    Hostname or IP address for the driver to listen on.
  </td>
</tr>
<tr>
  <td><code>spark.driver.port</code></td>
  <td>(random)</td>
  <td>
    Port for the driver to listen on.
  </td>
</tr>
<tr>
  <td><code>spark.akka.frameSize</code></td>
  <td>10</td>
  <td>
    Maximum message size to allow in "control plane" communication (for serialized tasks and task
    results), in MB. Increase this if your tasks need to send back large results to the driver
    (e.g. using <code>collect()</code> on a large dataset).
  </td>
</tr>
<tr>
  <td><code>spark.akka.threads</code></td>
  <td>4</td>
  <td>
    Number of actor threads to use for communication. Can be useful to increase on large clusters
    when the driver has a lot of CPU cores.
  </td>
</tr>
<tr>
  <td><code>spark.akka.timeout</code></td>
  <td>100</td>
  <td>
    Communication timeout between Spark nodes, in seconds.
  </td>
</tr>
<tr>
  <td><code>spark.akka.heartbeat.pauses</code></td>
  <td>600</td>
  <td>
     This is set to a larger value to disable failure detector that comes inbuilt akka. It can be
     enabled again, if you plan to use this feature (Not recommended). Acceptable heart beat pause
     in seconds for akka. This can be used to control sensitivity to gc pauses. Tune this in
     combination of `spark.akka.heartbeat.interval` and `spark.akka.failure-detector.threshold`
     if you need to.
  </td>
</tr>
<tr>
  <td><code>spark.akka.failure-detector.threshold</code></td>
  <td>300.0</td>
  <td>
     This is set to a larger value to disable failure detector that comes inbuilt akka. It can be
     enabled again, if you plan to use this feature (Not recommended). This maps to akka's
     `akka.remote.transport-failure-detector.threshold`. Tune this in combination of
     `spark.akka.heartbeat.pauses` and `spark.akka.heartbeat.interval` if you need to.
  </td>
</tr>
<tr>
  <td><code>spark.akka.heartbeat.interval</code></td>
  <td>1000</td>
  <td>
    This is set to a larger value to disable failure detector that comes inbuilt akka. It can be
    enabled again, if you plan to use this feature (Not recommended). A larger interval value in
    seconds reduces network overhead and a smaller value ( ~ 1 s) might be more informative for
    akka's failure detector. Tune this in combination of `spark.akka.heartbeat.pauses` and
    `spark.akka.failure-detector.threshold` if you need to. Only positive use case for using
    failure detector can be, a sensistive failure detector can help evict rogue executors really
    quick. However this is usually not the case as gc pauses and network lags are expected in a
    real Spark cluster. Apart from that enabling this leads to a lot of exchanges of heart beats
    between nodes leading to flooding the network with those.
  </td>
</tr>
</table>

#### Scheduling
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.task.cpus</code></td>
  <td>1</td>
  <td>
    Number of cores to allocate for each task.
  </td>
</tr>
<tr>
  <td><code>spark.task.maxFailures</code></td>
  <td>4</td>
  <td>
    Number of individual task failures before giving up on the job.
    Should be greater than or equal to 1. Number of allowed retries = this value - 1.
  </td>
</tr>
<tr>
  <td><code>spark.scheduler.mode</code></td>
  <td>FIFO</td>
  <td>
    The <a href="job-scheduling.html#scheduling-within-an-application">scheduling mode</a> between
    jobs submitted to the same SparkContext. Can be set to <code>FAIR</code>
    to use fair sharing instead of queueing jobs one after another. Useful for
    multi-user services.
  </td>
</tr>
<tr>
  <td><code>spark.cores.max</code></td>
  <td>(not set)</td>
  <td>
    When running on a <a href="spark-standalone.html">standalone deploy cluster</a> or a
    <a href="running-on-mesos.html#mesos-run-modes">Mesos cluster in "coarse-grained"
    sharing mode</a>, the maximum amount of CPU cores to request for the application from
    across the cluster (not from each machine). If not set, the default will be
    <code>spark.deploy.defaultCores</code> on Spark's standalone cluster manager, or
    infinite (all available cores) on Mesos.
  </td>
</tr>
<tr>
  <td><code>spark.mesos.coarse</code></td>
  <td>false</td>
  <td>
    If set to "true", runs over Mesos clusters in
    <a href="running-on-mesos.html#mesos-run-modes">"coarse-grained" sharing mode</a>,
    where Spark acquires one long-lived Mesos task on each machine instead of one Mesos task per
    Spark task. This gives lower-latency scheduling for short queries, but leaves resources in use
    for the whole duration of the Spark job.
  </td>
</tr>
<tr>
  <td><code>spark.speculation</code></td>
  <td>false</td>
  <td>
    If set to "true", performs speculative execution of tasks. This means if one or more tasks are
    running slowly in a stage, they will be re-launched.
  </td>
</tr>
<tr>
  <td><code>spark.speculation.interval</code></td>
  <td>100</td>
  <td>
    How often Spark will check for tasks to speculate, in milliseconds.
  </td>
</tr>
<tr>
  <td><code>spark.speculation.quantile</code></td>
  <td>0.75</td>
  <td>
    Percentage of tasks which must be complete before speculation is enabled for a particular stage.
  </td>
</tr>
<tr>
  <td><code>spark.speculation.multiplier</code></td>
  <td>1.5</td>
  <td>
    How many times slower a task is than the median to be considered for speculation.
  </td>
</tr>
<tr>
  <td><code>spark.locality.wait</code></td>
  <td>3000</td>
  <td>
    Number of milliseconds to wait to launch a data-local task before giving up and launching it
    on a less-local node. The same wait will be used to step through multiple locality levels
    (process-local, node-local, rack-local and then any). It is also possible to customize the
    waiting time for each level by setting <code>spark.locality.wait.node</code>, etc.
    You should increase this setting if your tasks are long and see poor locality, but the
    default usually works well.
  </td>
</tr>
<tr>
  <td><code>spark.locality.wait.process</code></td>
  <td>spark.locality.wait</td>
  <td>
    Customize the locality wait for process locality. This affects tasks that attempt to access
    cached data in a particular executor process.
  </td>
</tr>
<tr>
  <td><code>spark.locality.wait.node</code></td>
  <td>spark.locality.wait</td>
  <td>
    Customize the locality wait for node locality. For example, you can set this to 0 to skip
    node locality and search immediately for rack locality (if your cluster has rack information).
  </td>
</tr>
<tr>
  <td><code>spark.locality.wait.rack</code></td>
  <td>spark.locality.wait</td>
  <td>
    Customize the locality wait for rack locality.
  </td>
</tr>
<tr>
  <td><code>spark.scheduler.revive.interval</code></td>
  <td>1000</td>
  <td>
    The interval length for the scheduler to revive the worker resource offers to run tasks.
    (in milliseconds)
  </td>
</tr>
</table>

#### Security
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.authenticate</code></td>
  <td>false</td>
  <td>
    Whether Spark authenticates its internal connections. See
    <code>spark.authenticate.secret</code> if not running on YARN.
  </td>
</tr>
<tr>
  <td><code>spark.authenticate.secret</code></td>
  <td>None</td>
  <td>
    Set the secret key used for Spark to authenticate between components. This needs to be set if
    not running on YARN and authentication is enabled.
  </td>
</tr>
<tr>
  <td><code>spark.core.connection.auth.wait.timeout</code></td>
  <td>30</td>
  <td>
    Number of seconds for the connection to wait for authentication to occur before timing
    out and giving up.
  </td>
</tr>
<tr>
  <td><code>spark.ui.filters</code></td>
  <td>None</td>
  <td>
    Comma separated list of filter class names to apply to the Spark web UI. The filter should be a
    standard <a href="http://docs.oracle.com/javaee/6/api/javax/servlet/Filter.html">
    javax servlet Filter</a>. Parameters to each filter can also be specified by setting a
    java system property of: <br />
    <code>spark.&lt;class name of filter&gt;.params='param1=value1,param2=value2'</code><br />
    For example: <br />
    <code>-Dspark.ui.filters=com.test.filter1</code> <br />
    <code>-Dspark.com.test.filter1.params='param1=foo,param2=testing'</code>
  </td>
</tr>
<tr>
  <td><code>spark.ui.acls.enable</code></td>
  <td>false</td>
  <td>
    Whether Spark web ui acls should are enabled. If enabled, this checks to see if the user has
    access permissions to view the web ui. See <code>spark.ui.view.acls</code> for more details.
    Also note this requires the user to be known, if the user comes across as null no checks
    are done. Filters can be used to authenticate and set the user.
  </td>
</tr>
<tr>
  <td><code>spark.ui.view.acls</code></td>
  <td>Empty</td>
  <td>
    Comma separated list of users that have view access to the Spark web ui. By default only the
    user that started the Spark job has view access.
  </td>
</tr>
</table>

#### Spark Streaming
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.streaming.blockInterval</code></td>
  <td>200</td>
  <td>
    Interval (milliseconds) at which data received by Spark Streaming receivers is coalesced
    into blocks of data before storing them in Spark.
  </td>
</tr>
<tr>
  <td><code>spark.streaming.unpersist</code></td>
  <td>true</td>
  <td>
    Force RDDs generated and persisted by Spark Streaming to be automatically unpersisted from
    Spark's memory. The raw input data received by Spark Streaming is also automatically cleared.
    Setting this to false will allow the raw data and persisted RDDs to be accessible outside the
    streaming application as they will not be cleared automatically. But it comes at the cost of
    higher memory usage in Spark.
  </td>
</tr>
<tr>
  <td><code>spark.executor.logs.rolling.strategy</code></td>
  <td>(none)</td>
  <td>
    Set the strategy of rolling of executor logs. By default it is disabled. It can
    be set to "time" (time-based rolling) or "size" (size-based rolling). For "time",
    use <code>spark.executor.logs.rolling.time.interval</code> to set the rolling interval.
    For "size", use <code>spark.executor.logs.rolling.size.maxBytes</code> to set
    the maximum file size for rolling.
  </td>
</tr>
<tr>
  <td><code>spark.executor.logs.rolling.time.interval</code></td>
  <td>daily</td>
  <td>
    Set the time interval by which the executor logs will be rolled over.
    Rolling is disabled by default. Valid values are `daily`, `hourly`, `minutely` or
    any interval in seconds. See <code>spark.executor.logs.rolling.maxRetainedFiles</code>
    for automatic cleaning of old logs.
  </td>
</tr>
<tr>
  <td><code>spark.executor.logs.rolling.size.maxBytes</code></td>
  <td>(none)</td>
  <td>
    Set the max size of the file by which the executor logs will be rolled over.
    Rolling is disabled by default. Value is set in terms of bytes.
    See <code>spark.executor.logs.rolling.maxRetainedFiles</code>
    for automatic cleaning of old logs.
  </td>
</tr>
<tr>
  <td><code>spark.executor.logs.rolling.maxRetainedFiles</code></td>
  <td>(none)</td>
  <td>
    Sets the number of latest rolling log files that are going to be retained by the system.
    Older log files will be deleted. Disabled by default.
  </td>
</tr>
</table>

#### Cluster Managers
Each cluster manager in Spark has additional configuration options. Configurations 
can be found on the pages for each mode:

 * [YARN](running-on-yarn.html#configuration)
 * [Mesos](running-on-mesos.html)
 * [Standalone Mode](spark-standalone.html#cluster-launch-scripts)

# Environment Variables

Certain Spark settings can be configured through environment variables, which are read from the
`conf/spark-env.sh` script in the directory where Spark is installed (or `conf/spark-env.cmd` on
Windows). In Standalone and Mesos modes, this file can give machine specific information such as
hostnames. It is also sourced when running local Spark applications or submission scripts.

Note that `conf/spark-env.sh` does not exist by default when Spark is installed. However, you can
copy `conf/spark-env.sh.template` to create it. Make sure you make the copy executable.

The following variables can be set in `spark-env.sh`:


<table class="table">
  <tr><th style="width:21%">Environment Variable</th><th>Meaning</th></tr>
  <tr>
    <td><code>JAVA_HOME</code></td>
    <td>Location where Java is installed (if it's not on your default `PATH`).</td>
  </tr>
  <tr>
    <td><code>PYSPARK_PYTHON</code></td>
    <td>Python binary executable to use for PySpark.</td>
  </tr>
  <tr>
    <td><code>SPARK_LOCAL_IP</code></td>
    <td>IP address of the machine to bind to.</td>
  </tr>
  <tr>
    <td><code>SPARK_PUBLIC_DNS</code></td>
    <td>Hostname your Spark program will advertise to other machines.</td>
  </tr>
</table>

In addition to the above, there are also options for setting up the Spark
[standalone cluster scripts](spark-standalone.html#cluster-launch-scripts), such as number of cores
to use on each machine and maximum memory.

Since `spark-env.sh` is a shell script, some of these can be set programmatically -- for example, you might
compute `SPARK_LOCAL_IP` by looking up the IP of a specific network interface.

# Configuring Logging

Spark uses [log4j](http://logging.apache.org/log4j/) for logging. You can configure it by adding a
`log4j.properties` file in the `conf` directory. One way to start is to copy the existing
`log4j.properties.template` located there.
