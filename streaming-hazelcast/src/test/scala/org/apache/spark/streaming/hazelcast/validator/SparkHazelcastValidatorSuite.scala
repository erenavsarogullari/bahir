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

import java.io.FileNotFoundException
import java.util.Properties

import com.hazelcast.config.FileSystemXmlConfig
import com.hazelcast.core.{Hazelcast, HazelcastInstance}

import org.apache.spark.streaming.hazelcast.DistributedEventType
import org.apache.spark.streaming.hazelcast.SparkHazelcastConstants._
import org.apache.spark.SparkFunSuite

class SparkHazelcastValidatorSuite extends SparkFunSuite {

  private val HazelcastXMLConfigFile = "src/test/resources/hazelcast_test_config.xml"
  private var hazelcastInstance: HazelcastInstance = _

  override def beforeAll() {
    hazelcastInstance =
      Hazelcast.getOrCreateHazelcastInstance(
        new FileSystemXmlConfig("src/test/resources/hazelcast_test_config.xml"))
  }

  override def afterAll() {
    hazelcastInstance.shutdown()
  }

  test("Validate 'hazelcast.xml.config.file.name' property when it is set as empty.") {
    val properties = new Properties()
    properties.put(HazelcastXMLConfigFileName, "")

    val ex = intercept[IllegalArgumentException] {
      SparkHazelcastValidator.validateProperties(properties)
    }

    assert(ex.getMessage == s"'$HazelcastXMLConfigFileName' property can not be blank.")
  }

  test("Validate 'hazelcast.xml.config.file.name' property when it is set as non-existent file.") {
    val properties = new Properties()
    properties.put(HazelcastXMLConfigFileName, "nonexistent_hazelcast_test_config.xml")

    val ex = intercept[FileNotFoundException] {
      SparkHazelcastValidator.validateProperties(properties)
    }

    assert(ex.getMessage ==
                  "nonexistent_hazelcast_test_config.xml (No such file or directory)")
  }

  test("Validate properties when 'hazelcast.distributed.object.name' property is set as empty.") {
    val properties = new Properties()
    properties.put(HazelcastXMLConfigFileName, HazelcastXMLConfigFile)
    properties.put(HazelcastDistributedObjectName, "")

    val ex = intercept[IllegalArgumentException] {
      SparkHazelcastValidator.validateProperties(properties)
    }

    assert(ex.getMessage == s"'$HazelcastDistributedObjectName' property can not be blank.")
  }

  test("Validate properties when 'hazelcast.distributed.object.type' property is set as empty.") {
    val properties = new Properties()
    properties.put(HazelcastXMLConfigFileName, HazelcastXMLConfigFile)
    properties.put(HazelcastDistributedObjectName, "test_distributed_map")
    properties.put(HazelcastDistributedObjectType, "")

    val ex = intercept[IllegalArgumentException] {
      SparkHazelcastValidator.validateProperties(properties)
    }

    assert(ex.getMessage ==
            s"'$HazelcastDistributedObjectType' property must be instanceOf DistributedObjectType")
  }

  test("Validate DistributedEventTypes for Map Structure when distributed object type " +
                                                                                "is not matched.") {
    val ex = intercept[IllegalStateException] {
      SparkHazelcastValidator.validateDistributedEventTypesOfMap(
          hazelcastInstance.getList("test_hz_distributed_list"), Set(DistributedEventType.Updated))
    }

    assert(ex.getMessage == "Expected Distributed Object Types : [IMap, MultiMap and " +
                                              "ReplicatedMap] but test_hz_distributed_list found!")
  }

  test("Validate DistributedEventTypes for Map Structure when event set is empty.") {
    val ex = intercept[IllegalArgumentException] {
      SparkHazelcastValidator.validateDistributedEventTypesOfMap(
                                        hazelcastInstance.getMap("test_hz_distributed_map"), Set())
    }

    assert(ex.getMessage == "'distributedEventTypes' can not be empty. Supported values: " +
                            s"[${DistributedEventType.Added}, ${DistributedEventType.Removed}, " +
                            s"${DistributedEventType.Updated}, ${DistributedEventType.Evicted}]")
  }

  test("Validate DistributedEventTypes for Map Structure when event set contains null element.") {
    val ex = intercept[IllegalArgumentException] {
      SparkHazelcastValidator
        .validateDistributedEventTypesOfMap(hazelcastInstance.getMap("test_hz_distributed_map"),
                                            Set(DistributedEventType.Updated, null))
    }

    assert(ex.getMessage == "'distributedEventTypes' can not contain null element.")
  }

  test("Validate DistributedEventTypes for Distributed List/Set/Queue " +
                                                  "when distributed object type is not matched.") {
    val ex = intercept[IllegalStateException] {
      SparkHazelcastValidator
        .validateDistributedEventTypes(hazelcastInstance.getMap("test_hz_distributed_map"),
                                        Set(DistributedEventType.Added))
    }

    assert(ex.getMessage == "Expected Distributed Object Types : [IList, ISet and IQueue] " +
                                                              "but test_hz_distributed_map found!")
  }

  test("Validate DistributedEventTypes for Distributed List/Set/Queue when event set is empty.") {
    val ex = intercept[IllegalArgumentException] {
      SparkHazelcastValidator
        .validateDistributedEventTypes(hazelcastInstance.getList("test_hz_distributed_list"), Set())
    }

    assert(ex.getMessage == "'distributedEventTypes' can not be empty. Supported values: " +
      s"[${DistributedEventType.Added}, ${DistributedEventType.Removed}]")
  }

  test("Validate DistributedEventTypes for Distributed List/Set/Queue " +
                                                          "when event set contains null element.") {
    val ex = intercept[IllegalArgumentException] {
      SparkHazelcastValidator
        .validateDistributedEventTypes(hazelcastInstance.getList("test_hz_distributed_list"),
                                        Set(DistributedEventType.Added, null))
    }

    assert(ex.getMessage == "'distributedEventTypes' can not contain null element.")
  }

  test("Validate DistributedEventTypes for Distributed List/Set/Queue " +
                                                                  "when event type is not valid.") {
    val ex = intercept[IllegalArgumentException] {
      SparkHazelcastValidator
        .validateDistributedEventTypes(hazelcastInstance.getList("test_hz_distributed_list"),
                                        Set(DistributedEventType.Updated))
    }

    assert(ex.getMessage == s"Expected Distributed Event Types: [${DistributedEventType.Added}, " +
      s"${DistributedEventType.Removed}] but Updated found!")
  }

}
