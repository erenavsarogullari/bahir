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

package org.apache.spark.streaming.hazelcast.validator

import java.util.Properties

import com.hazelcast.config.FileSystemXmlConfig
import com.hazelcast.core._
import org.apache.commons.lang3.Validate

import org.apache.spark.streaming.hazelcast.DistributedEventType
import org.apache.spark.streaming.hazelcast.DistributedEventType.DistributedEventType
import org.apache.spark.streaming.hazelcast.DistributedObjectType.DistributedObjectType
import org.apache.spark.streaming.hazelcast.SparkHazelcastConstants._

private[hazelcast] object SparkHazelcastValidator {

  val supportedDistributedEventTypesOfMaps: Set[DistributedEventType] =
    Set(DistributedEventType.Added,
        DistributedEventType.Removed,
        DistributedEventType.Updated,
        DistributedEventType.Evicted)

  val supportedDistributedEventTypes: Set[DistributedEventType] =
    Set(DistributedEventType.Added,
        DistributedEventType.Removed)

  def validateProperties(properties: Properties) {
    Validate.notBlank(properties.getProperty(HazelcastXMLConfigFileName),
      s"'$HazelcastXMLConfigFileName' property can not be blank.")
    Validate.notNull(new FileSystemXmlConfig(properties.getProperty(HazelcastXMLConfigFileName)),
      s"'$HazelcastXMLConfigFileName' property can not be null.")
    Validate.notBlank(properties.getProperty(HazelcastDistributedObjectName),
      s"'$HazelcastDistributedObjectName' property can not be blank.")
    if (!properties.get(HazelcastDistributedObjectType).isInstanceOf[DistributedObjectType]) {
      throw new IllegalArgumentException(s"'$HazelcastDistributedObjectType' property must be " +
        "instanceOf DistributedObjectType")
    }
  }

  def validateDistributedEventTypesOfMap
  (distributedObject: DistributedObject, distributedEventTypes: Set[DistributedEventType]) {
    distributedObject match {
      case hzMap: IMap[_, _] =>
        checkDistributedEventTypes(distributedEventTypes, supportedDistributedEventTypesOfMaps)
      case multiMap: MultiMap[_, _] =>
        checkDistributedEventTypes(distributedEventTypes, supportedDistributedEventTypesOfMaps)
      case replicatedMap: ReplicatedMap[_, _] =>
        checkDistributedEventTypes(distributedEventTypes, supportedDistributedEventTypesOfMaps)
      case distObj: Any => throw new IllegalStateException(s"Expected Distributed Object Types : " +
        s"[IMap, MultiMap and ReplicatedMap] but ${distObj.getName} found!")
    }
  }

  def validateDistributedEventTypes(distributedObject: DistributedObject,
                                       distributedEventTypes: Set[DistributedEventType]) : Unit = {
    distributedObject match {
      case hzList: IList[_] =>
        checkDistributedEventTypes(distributedEventTypes, supportedDistributedEventTypes)

      case hzSet: ISet[_] =>
        checkDistributedEventTypes(distributedEventTypes, supportedDistributedEventTypes)

      case hzQueue: IQueue[_] =>
        checkDistributedEventTypes(distributedEventTypes, supportedDistributedEventTypes)

      case hzQueue: ITopic[_] =>

      case distObj: Any => throw new IllegalStateException(s"Expected Distributed Object Types : " +
        s"[IList, ISet and IQueue] but ${distObj.getName} found!")
    }
  }

  def checkDistributedEventTypes(distributedEventTypes: Set[DistributedEventType],
                                         supportedDistributedEventTypes: Set[DistributedEventType])
  {
    val supportedDistributedEventTypesAsString = supportedDistributedEventTypes.mkString(", ")
    Validate.notEmpty(distributedEventTypes.toArray, "'distributedEventTypes' can not be empty. " +
      s"Supported values: [$supportedDistributedEventTypesAsString]")
    Validate.noNullElements(distributedEventTypes.toArray, "'distributedEventTypes' can not " +
      "contain null element.")

    distributedEventTypes.foreach(eventType => {
      if (!supportedDistributedEventTypes.contains(eventType)) {
        throw new IllegalArgumentException(s"Expected Distributed Event Types: " +
          s"[$supportedDistributedEventTypesAsString] but $eventType found!")
      }
    })
  }

}
