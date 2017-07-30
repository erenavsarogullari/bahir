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

import com.hazelcast.query.Predicate

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.hazelcast.DistributedEventType.DistributedEventType

object HazelcastUtils {

   /**
    * Create an input stream that receives messages by registering event listener.
    * IMap, MultiMap and ReplicatedMap are supported distributed data structures.
    *
    * This Receiver will start to receive the message arrived after the listener registered.
    *
    * @param ssc                   StreamingContext object
    * @param storageLevel          RDD storage level. Default: StorageLevel.MEMORY_ONLY.
    * @param properties            Hazelcast Properties
    * @param distributedEventTypes Distributed Event Types to listen. Optional. Default: ADDED.
    * @param predicate             Predicate for filtering entries. Optional. Default: None.
    * @param key                   Key to listen for. Optional. Default: None.
    * @return HazelcastPairInputDStream instance
    */
  def createHazelcastEntryStream[K, V](ssc: StreamingContext,
                                       storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
                                       properties: Properties,
                                       distributedEventTypes: Set[DistributedEventType]
                                       = Set(DistributedEventType.Added),
                                       predicate: Option[Predicate[K, V]] = None,
                                       key: Option[K] = None): HazelcastPairInputDStream[K, V] = {

    new HazelcastPairInputDStream[K, V](ssc,
      storageLevel,
      properties,
      distributedEventTypes,
      predicate,
      key)

  }

   /**
    * Create an input stream that receives messages by registering item listener.
    * IList, ISet and IQueue are supported distributed data structures.
    *
    * This Receiver will start to receive the message arrived after the listener registered.
    *
    * @param ssc                   StreamingContext object
    * @param storageLevel          RDD storage level. Default: StorageLevel.MEMORY_ONLY.
    * @param properties            Hazelcast Properties
    * @param distributedEventTypes Distributed Event Types to listen. Optional. Default: ADDED.
    * @return HazelcastInputDStream instance
    */
  def createHazelcastItemStream[T](ssc: StreamingContext,
                                   storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
                                   properties: Properties,
                                   distributedEventTypes: Set[DistributedEventType]
                                   = Set(DistributedEventType.Added)): HazelcastInputDStream[T] = {

    new HazelcastInputDStream[T](ssc, storageLevel, properties, distributedEventTypes)

  }

   /**
    * Create an input stream that receives messages by registering item listener.
    * ITopic is supported distributed data structures.
    *
    * This Receiver will start to receive the message arrived after the listener registered.
    *
    * @param ssc          StreamingContext object
    * @param storageLevel RDD storage level. Default: StorageLevel.MEMORY_ONLY.
    * @param properties   Hazelcast Properties
    * @return HazelcastInputDStream instance
    */
  def createHazelcastMessageStream[T](ssc: StreamingContext,
                                      storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
                                      properties: Properties): HazelcastInputDStream[T] = {

    new HazelcastInputDStream[T](ssc, storageLevel, properties, Set())

  }

}
