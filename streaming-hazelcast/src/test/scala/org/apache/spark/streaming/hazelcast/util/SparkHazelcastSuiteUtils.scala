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

package org.apache.spark.streaming.hazelcast.util

import java.util.Properties

import scala.collection.mutable.Queue
import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.hazelcast.DistributedObjectType.DistributedObjectType
import org.apache.spark.streaming.hazelcast.config.SparkHazelcastService._

object SparkHazelcastSuiteUtils {

  val HazelcastXMLFileName = "hazelcast_test_config.xml"

  def getExpectedTupleList(count: Int): List[(Int, String)] =
                                                (1 to count).map(i => (i, s"test_value_$i")).toList

  def getExpectedList(count: Int): List[Int] = (1 to count).toList

  def getProperties(distributedObjectType: DistributedObjectType, distributedObjectName: String)
                                                                                    : Properties = {
    val properties = new Properties()
    properties.put(HazelcastXMLConfigFileName, HazelcastXMLFileName)
    properties.put(HazelcastDistributedObjectName, distributedObjectName)
    properties.put(HazelcastDistributedObjectType, distributedObjectType)
    properties
  }

  def createRDDQueue[T: ClassTag](sc: SparkContext, expectedList: List[T]): Queue[RDD[T]] = {
    val rddQueue: Queue[RDD[T]] = Queue()
    val tempExpectedBuffer = expectedList.toBuffer
    for (i <- 1 to (expectedList.size / 2)) {
      val intRDD = sc.parallelize[T](tempExpectedBuffer.take(2))
      rddQueue += intRDD
      tempExpectedBuffer.remove(0, 2)
    }
    rddQueue
  }

}
