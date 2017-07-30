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

package org.apache.spark.streaming.hazelcast.config

import java.util.Properties

import com.hazelcast.config.FileSystemXmlConfig
import com.hazelcast.core._

import org.apache.spark.streaming.hazelcast.DistributedObjectType
import org.apache.spark.streaming.hazelcast.DistributedObjectType.DistributedObjectType
import org.apache.spark.streaming.hazelcast.SparkHazelcastConstants._
import org.apache.spark.streaming.hazelcast.validator.SparkHazelcastValidator

private[hazelcast] object SparkHazelcastService {

  def getDistributedObject(properties: Properties): DistributedObject = {
    SparkHazelcastValidator.validateProperties(properties)

    val hazelcastXMLConfigFileName = properties.getProperty(HazelcastXMLConfigFileName)
    val hazelcastDistributedObjectName = properties.getProperty(HazelcastDistributedObjectName)
    val hazelcastDistributedObjectType = properties.get(HazelcastDistributedObjectType)
                                                                .asInstanceOf[DistributedObjectType]

    val config = new FileSystemXmlConfig(hazelcastXMLConfigFileName)
    val hazelcastInstance = Hazelcast.getOrCreateHazelcastInstance(config)

    getDistributedObject(hazelcastInstance,
                          hazelcastDistributedObjectType,
                          hazelcastDistributedObjectName)
  }

  private def getDistributedObject(hazelcastInstance: HazelcastInstance,
                                   hazelcastDistributedObjectType: DistributedObjectType,
                                   hazelcastDistributedObjectName: String): DistributedObject = {
    hazelcastDistributedObjectType match {
      case DistributedObjectType.IMap => hazelcastInstance.getMap(hazelcastDistributedObjectName)
      case DistributedObjectType.MultiMap =>
        hazelcastInstance.getMultiMap(hazelcastDistributedObjectName)
      case DistributedObjectType.ReplicatedMap =>
        hazelcastInstance.getReplicatedMap(hazelcastDistributedObjectName)
      case DistributedObjectType.IList => hazelcastInstance.getList(hazelcastDistributedObjectName)
      case DistributedObjectType.ISet => hazelcastInstance.getSet(hazelcastDistributedObjectName)
      case DistributedObjectType.IQueue =>
        hazelcastInstance.getQueue(hazelcastDistributedObjectName)
      case DistributedObjectType.ITopic =>
        hazelcastInstance.getTopic(hazelcastDistributedObjectName)
      case DistributedObjectType.ReliableTopic =>
        hazelcastInstance.getReliableTopic(hazelcastDistributedObjectName)
      case distObj: Any => throw new IllegalStateException(s"Expected Distributed Object Types : " +
        s"[IMap, MultiMap, ReplicatedMap, IList, ISet and IQueue, ITopic and ReliableTopic] but " +
          s"$hazelcastDistributedObjectName found!")
    }
  }

}
