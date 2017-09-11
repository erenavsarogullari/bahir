/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.hazelcast

import java.util.Properties

import com.hazelcast.config.FileSystemXmlConfig
import com.hazelcast.core.{Hazelcast, IMap}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.hazelcast.SparkHazelcastConstants._
import org.apache.spark.streaming.hazelcast.dstream.HazelcastPairInputDStream

private object WriteDistributedMapToDStream {

  val HazelcastXMLFileName = "hazelcast_config.xml"
  val HazelcastDistributedMapName = "test_distributed_map"
  val HazelcastXMLFilePath = getClass().getClassLoader().getResource(HazelcastXMLFileName).getPath()

  def main(args: Array[String]) {

    // Hazelcast Distributed Map Events Stream is started...
    new Thread(new HazelcastDistributedEventStreamTask).start()

    // Spark Context is created...
    val sc = new SparkContext(new SparkConf().setAppName("spark-hazelcast").setMaster("local[2]"))
    // Spark Streaming Context is created...
    val ssc = new StreamingContext(sc, Seconds(2))

    // Spark Hazelcast properties are created...
    val sparkHazelcastProperties = new Properties()
    sparkHazelcastProperties.put(HazelcastXMLConfigFileName, HazelcastXMLFilePath)
    sparkHazelcastProperties.put(HazelcastDistributedObjectName, HazelcastDistributedMapName)
    sparkHazelcastProperties.put(HazelcastDistributedObjectType, DistributedObjectType.IMap)

    // Distributed Map Events are written to Spark as the DStream...
    val hzMapStream: HazelcastPairInputDStream[Int, String] =
      HazelcastUtils.createHazelcastEntryStream[Int, String](ssc,
                                                              StorageLevel.MEMORY_ONLY,
                                                              sparkHazelcastProperties,
                                                              Set(DistributedEventType.Added,
                                                                  DistributedEventType.Updated,
                                                                  DistributedEventType.Removed))

    // Prints stream content...
    hzMapStream.print(20)

    // Spark Streaming Context is started...
    ssc.start()
    ssc.awaitTermination()
  }

  private class HazelcastDistributedEventStreamTask extends Runnable {

    override def run(): Unit = {
      // Distributed Map is created with stream content...
      val hzInstance = Hazelcast.getOrCreateHazelcastInstance(
                                                      new FileSystemXmlConfig(HazelcastXMLFilePath))
      val distributedMap: IMap[Int, User] = hzInstance.getMap(HazelcastDistributedMapName)
      (1 to 1000).foreach(index => {
        Thread.sleep(1000)
        distributedMap.put(index, new User(index, s"name$index", s"surname$index"))
        distributedMap.put(index, new User(index,
                                            s"name${index}_updated",
                                            s"surname${index}_updated"))
        distributedMap.remove(index)
      })
    }

  }

}
