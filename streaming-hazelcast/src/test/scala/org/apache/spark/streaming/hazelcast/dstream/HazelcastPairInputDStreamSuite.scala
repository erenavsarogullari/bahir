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

package org.apache.spark.streaming.hazelcast.dstream

import java.util.Properties
import java.util.concurrent.CountDownLatch

import com.hazelcast.core._
import org.apache.commons.lang3.StringUtils
import org.scalatest.BeforeAndAfter

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.hazelcast.{DistributedEventType, DistributedObjectType, HazelcastUtils}
import org.apache.spark.streaming.hazelcast.DistributedEventType.DistributedEventType
import org.apache.spark.streaming.hazelcast.DistributedObjectType.DistributedObjectType
import org.apache.spark.streaming.hazelcast.config.SparkHazelcastService
import org.apache.spark.streaming.hazelcast.util.SparkHazelcastSuite
import org.apache.spark.streaming.hazelcast.util.SparkHazelcastSuiteUtils._

class HazelcastPairInputDStreamSuite extends SparkHazelcastSuite with BeforeAndAfter {

  before {
    startStreamingContext()
  }

  after {
    stopStreamingContext()
  }

  test("A Distributed Map having 10 integers should be written to Spark as DStream") {
    test(DistributedObjectType.IMap, "test_distributed_map4", 10)
  }

  test("A MultiMap having 15 integers should be written to Spark as DStream") {
    test(DistributedObjectType.MultiMap, "test_distributed_multiMap4", 15)
  }

  test("A ReplicatedMap having 20 integers should be written to Spark as DStream") {
    test(DistributedObjectType.ReplicatedMap, "test_distributed_replicatedMap4", 20)
  }

  private def test(distributedObjectType: DistributedObjectType,
                   distributedObjectName: String,
                   expectedMessageCount: Int,
                   distributedEventTypes: Set[DistributedEventType] =
                      Set(DistributedEventType.Added,
                          DistributedEventType.Removed,
                          DistributedEventType.Updated)) {
    val expectedTupleList = getExpectedTupleList(expectedMessageCount)

    val properties = getProperties(distributedObjectType, distributedObjectName)

    new Thread(new HazelcastEntryLoader[Int, String](properties, expectedTupleList)).start()

    val latch = new CountDownLatch(expectedTupleList.size)

    val hazelcastEntryStream = HazelcastUtils
                                  .createHazelcastEntryStream[Int, String](ssc,
                                                              StorageLevel.MEMORY_ONLY, properties)

    verify[Int, String](hazelcastEntryStream, expectedTupleList, latch)

    ssc.start()

    latch.await()
  }

  private def verify[K, V](hazelcastEntryStream: HazelcastPairInputDStream[K, V],
                           expectedTupleList: List[(K, V)],
                           latch: CountDownLatch,
                           distributedEventTypes: Set[DistributedEventType] = Set
                           (DistributedEventType.Added)) {
    hazelcastEntryStream.foreachRDD(rdd => {
      rdd.collect().foreach {
        case(memberAddress, eventType, key, oldValue, value) =>
          assert(StringUtils.isNotBlank(memberAddress))
          assert(StringUtils.isNotBlank(eventType))
          assert(distributedEventTypes.contains(DistributedEventType.withName(eventType)))
          assert(expectedTupleList.contains((key, value)))
      }
      latch.countDown()
    })
  }

  private class HazelcastEntryLoader[Int, String](properties: Properties,
                                         expectedTupleList: List[(Int, String)]) extends Runnable {

    override def run(): Unit = {
      val distributedObject = SparkHazelcastService.getDistributedObject(properties)
      distributedObject match {
        case hzMap: IMap[Int, String] =>
          expectedTupleList.foreach {
            case (key, value) =>
              hzMap.put(key, value)
              hzMap.put(key, value)
              hzMap.remove(key)
              Thread.sleep(BatchDuration)
          }

        case multiMap: MultiMap[Int, String] =>
          expectedTupleList.foreach {
            case (key, value) =>
                multiMap.put(key, value)
                multiMap.remove(key)
                Thread.sleep(BatchDuration)
          }

        case replicatedMap: ReplicatedMap[Int, String] =>
          expectedTupleList.foreach {
            case (key, value) =>
              replicatedMap.put(key, value)
              replicatedMap.put(key, value)
              replicatedMap.remove(key)
              Thread.sleep(BatchDuration)
          }

        case distObj: Any => fail(s"Expected Distributed Object Types: " +
                              s"[IMap, MultiMap and ReplicatedMap] but ${distObj.getName} found!")
      }
    }

  }

}
