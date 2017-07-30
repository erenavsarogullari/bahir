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

import com.hazelcast.core.ITopic
import org.apache.commons.lang3.StringUtils
import org.scalatest.BeforeAndAfter

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.hazelcast.{DistributedObjectType, HazelcastUtils}
import org.apache.spark.streaming.hazelcast.DistributedObjectType.DistributedObjectType
import org.apache.spark.streaming.hazelcast.config.SparkHazelcastService
import org.apache.spark.streaming.hazelcast.util.SparkHazelcastSuite
import org.apache.spark.streaming.hazelcast.util.SparkHazelcastSuiteUtils._

class HazelcastMessageInputDStreamSuite extends SparkHazelcastSuite with BeforeAndAfter {

  before {
    startStreamingContext()
  }

  after {
    stopStreamingContext()
  }

  test("A Topic having 20 integers should be written to Spark as DStream") {
    test(DistributedObjectType.ITopic, "test_topic4", 20)
  }

  test("A Reliable Topic having 30 integers should be written to Spark as DStream") {
    test(DistributedObjectType.ReliableTopic, "test_reliableTopic4", 30)
  }

  private def test(distributedObjectType: DistributedObjectType,
                   distributedObjectName: String,
                   expectedMessageCount: Int) {
    val expectedList = getExpectedList(expectedMessageCount)

    val properties = getProperties(distributedObjectType, distributedObjectName)

    new Thread(new HazelcastMessageLoader[Int](properties, expectedList)).start()

    val latch = new CountDownLatch(expectedList.size)

    val hazelcastMessageStream = HazelcastUtils
                                      .createHazelcastMessageStream[Int](ssc,
                                                                          StorageLevel.MEMORY_ONLY,
                                                                          properties)

    verify[Int](hazelcastMessageStream, expectedList, latch)

    ssc.start()

    latch.await()
  }

  private def verify[T](hazelcastMessageStream: HazelcastInputDStream[T],
                        expectedList: List[T],
                        latch: CountDownLatch): Unit = {
    hazelcastMessageStream.foreachRDD(rdd => {
      rdd.collect().foreach {
        case(memberAddress, eventType, item) =>
          assert(StringUtils.isNotBlank(memberAddress))
          assert(StringUtils.isNotBlank(eventType))
          assert(expectedList.contains(item))
      }
      latch.countDown()
    })
  }

  private class HazelcastMessageLoader[T](properties: Properties, expectedList: List[T])
    extends Runnable {

    override def run(): Unit = {
      val distributedObject = SparkHazelcastService.getDistributedObject(properties)
      distributedObject match {
        case hzTopic: ITopic[T] =>
          expectedList.foreach(message => {
            hzTopic.publish(message)
            Thread.sleep(BatchDuration)
          })

        case distObj: Any => fail(s"Expected Distributed Object Types : [ITopic] " +
          s"but ${distObj.getName} found!")
      }
    }

  }

}
