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

import com.hazelcast.config.ClasspathXmlConfig
import com.hazelcast.core.{Hazelcast, IList}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.hazelcast.config.SparkHazelcastService._

object WriteDistributedItemToDStream {

  val HazelcastXMLFileName = "hazelcast_config.xml"
  val HazelcastDistributedListName = "test_distributed_list"

  def main(args: Array[String]) {
    // Hazelcast Distributed Item Events Stream is started...
    new Thread(new HazelcastDistributedEventStreamTask).start()

    // Spark Context is created...
    val sc = new SparkContext(new SparkConf().setAppName("spark-hazelcast").setMaster("local[2]"))

    // Spark Streaming Context is created...
    val ssc = new StreamingContext(sc, Seconds(2))

    // Spark Hazelcast properties are created...
    val sparkHazelcastProperties = new Properties()
    sparkHazelcastProperties.put(HazelcastXMLConfigFileName, HazelcastXMLFileName)
    sparkHazelcastProperties.put(HazelcastDistributedObjectName, HazelcastDistributedListName)
    sparkHazelcastProperties.put(HazelcastDistributedObjectType, DistributedObjectType.IList)

    // Distributed List Events are written to Spark as the DStream...
    val hzListStream = HazelcastUtils.createHazelcastItemStream[User](ssc,
                                                                    StorageLevel.MEMORY_ONLY,
                                                                    sparkHazelcastProperties,
                                                                    Set(DistributedEventType.ADDED,
                                                                    DistributedEventType.REMOVED))

    // Prints stream content...
    hzListStream.print(20)

    // Spark Streaming Context is started...
    ssc.start()
    ssc.awaitTermination()
  }

  private class HazelcastDistributedEventStreamTask extends Runnable {

    override def run(): Unit = {
      // Distributed List is created with stream content...
      val hzInstance = Hazelcast.getOrCreateHazelcastInstance(
                                                      new ClasspathXmlConfig(HazelcastXMLFileName))
      val distributedList: IList[User] = hzInstance.getList(HazelcastDistributedListName)
      (1 to 1000).foreach(index => {
        Thread.sleep(1000)
        val user = new User(index, s"name$index", s"surname$index")
        distributedList.add(user)
        distributedList.remove(user)
      })
    }

  }

}
