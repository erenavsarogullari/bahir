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

import com.hazelcast.core._

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.hazelcast.DistributedEventType
import org.apache.spark.streaming.hazelcast.DistributedEventType.DistributedEventType
import org.apache.spark.streaming.hazelcast.validator.SparkHazelcastValidator
import org.apache.spark.streaming.receiver.Receiver

class HazelcastInputDStream[T](ssc: StreamingContext,
                               storageLevel: StorageLevel,
                               properties: Properties,
                               distributedEventTypes: Set[DistributedEventType])
                                            extends ReceiverInputDStream[(String, String, T)](ssc) {

  override def getReceiver(): Receiver[(String, String, T)] =
                      new HazelcastReceiver[T](storageLevel, properties, distributedEventTypes)

}

private class HazelcastReceiver[T](storageLevel: StorageLevel,
                               properties: Properties,
                               distributedEventTypes: Set[DistributedEventType])
                        extends Receiver[(String, String, T)](storageLevel)
                        with HazelcastBaseReceiver {

  override protected val props: Properties = properties

  SparkHazelcastValidator.validateDistributedEventTypes(distributedObject, distributedEventTypes)

  override def onStart() {
    start()
  }

  override def onStop() {
    stop()
  }

  override protected def registerListener(distributedObject: DistributedObject): String = {
    distributedObject match {
      case hzList: IList[T] => hzList.addItemListener(
        new HazelcastInputDStreamItemListener[T](this, distributedEventTypes), true)
      case hzSet: ISet[T] => hzSet.addItemListener(
        new HazelcastInputDStreamItemListener[T](this, distributedEventTypes), true)
      case hzQueue: IQueue[T] => hzQueue.addItemListener(
        new HazelcastInputDStreamItemListener[T](this, distributedEventTypes), true)
      case hzTopic: ITopic[T] => hzTopic.addMessageListener(
        new HazelcastInputDStreamMessageListener[T](this))
      case distObj: Any => throw new IllegalStateException(s"Expected Distributed Object Types : " +
        s"[IList, ISet and IQueue] but ${distObj.getName} found!")
    }
  }

  override protected def unregisterListener(distributedObject: DistributedObject,
                                            registrationId: String) {
    distributedObject match {
      case hzList: IList[T] => hzList.removeItemListener(registrationId)
      case hzSet: ISet[T] => hzSet.removeItemListener(registrationId)
      case hzQueue: IQueue[T] => hzQueue.removeItemListener(registrationId)
      case hzTopic: ITopic[T] => hzTopic.removeMessageListener(registrationId)
    }
  }

  private class HazelcastInputDStreamItemListener[T]
                                              (val receiver: HazelcastReceiver[T],
                                               val distributedEventTypes: Set[DistributedEventType])
                                          extends ItemListener[T] {

    override def itemAdded(item: ItemEvent[T]) {
      store(item)
    }

    override def itemRemoved(item: ItemEvent[T]) {
      store(item)
    }

    private def store(item: ItemEvent[T]) {
      if (distributedEventTypes.contains(
        DistributedEventType.withName(item.getEventType.name().toLowerCase.capitalize))) {
          receiver.store((item.getMember.getAddress.toString,
                          item.getEventType.name(),
                          item.getItem))
        }
    }

  }

  private class HazelcastInputDStreamMessageListener[T](receiver: HazelcastReceiver[T])
                                              extends MessageListener[T] {

    override def onMessage(message: Message[T]) {
      receiver.store((message.getPublishingMember.getAddress.toString,
                      message.getPublishTime.toString,
                      message.getMessageObject))
    }

  }

}


