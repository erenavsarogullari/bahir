Spark Streaming - Hazelcast Support


A library for reading data from [Hazelcast](https://hazelcast.org/) using Spark Streaming. 

## Linking

Using SBT:

    libraryDependencies += "org.apache.bahir" %% "spark-streaming-hazelcast" % "2.3.0-SNAPSHOT"

Using Maven:

    <dependency>
        <groupId>org.apache.bahir</groupId>
        <artifactId>spark-streaming-hazelcast_2.11</artifactId>
        <version>2.3.0-SNAPSHOT</version>
    </dependency>

This library can also be added to Spark jobs launched through `spark-shell` or `spark-submit` by using the `--packages` command line option.
For example, to include it when starting the spark shell:

    $ bin/spark-shell --packages org.apache.bahir:spark-streaming-hazelcast_2.11:2.3.0-SNAPSHOT

Unlike using `--jars`, using `--packages` ensures that this library and its dependencies will be added to the classpath.
The `--packages` argument can also be used with `bin/spark-submit`.

This library is cross-published for Scala 2.10 and Scala 2.11, so users should replace the proper Scala version (2.10 or 2.11) in the commands listed above.

## Examples


### Scala API

Create Hazelcast Entry Streams. Supported distributed data structures are IMap, MultiMap and ReplicatedMap.

    import org.apache.spark.streaming.hazelcast._
    
    val properties = new Properties()
    properties.put("hazelcast.xml.config.file.name", "test_hazelcast_config.xml")
    properties.put("hazelcast.distributed.object.name", "test_distributed_map_name")
    properties.put("hazelcast.distributed.object.type", DistributedObjectType.IMap)
    
    val entries: HazelcastPairInputDStream[Int, String] = HazelcastUtils.createHazelcastEntryStream[Int, String](ssc, properties, ...)
    
Create Hazelcast Item Streams. Supported distributed data structures are IList, ISet and IQueue.

    import org.apache.spark.streaming.hazelcast._
    
    val properties = new Properties()
    properties.put("hazelcast.xml.config.file.name", "test_hazelcast_config.xml")
    properties.put("hazelcast.distributed.object.name", "test_distributed_list_name")
    properties.put("hazelcast.distributed.object.type", DistributedObjectType.IList)
    
    val items: HazelcastInputDStream[User] = HazelcastUtils.createHazelcastItemStream[User](ssc, properties, ...)
    
Create Hazelcast Message Streams. Supported distributed data structures are ITopic and ReliableTopic.

    import org.apache.spark.streaming.hazelcast._
    
    val properties = new Properties()
    properties.put("hazelcast.xml.config.file.name", "test_hazelcast_config.xml")
    properties.put("hazelcast.distributed.object.name", "test_distributed_topic_name")
    properties.put("hazelcast.distributed.object.type", DistributedObjectType.ITopic)
    
    val messages: HazelcastInputDStream[User] = HazelcastUtils.createHazelcastMessageStream[User](ssc, properties, ...)

### Java API

    In Progress

See end-to-end examples at [Hazelcast Examples](https://github.com/apache/bahir/tree/master/streaming-hazelcast/examples)