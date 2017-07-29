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

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

private[hazelcast] trait SparkHazelcastSuite extends SparkFunSuite {

  private lazy val sparkConf = new SparkConf().setAppName("spark-hazelcast").setMaster("local[2]")
  protected val BatchDuration = 5
  protected var sc: SparkContext = _
  protected var ssc: StreamingContext = _

  protected def startSparkContext() {
    sc = new SparkContext(sparkConf)
  }

  protected def stopSparkContext() {
    sc.stop()
  }

  protected def startStreamingContext() {
    sc = new SparkContext(sparkConf)
    ssc = new StreamingContext(sc, Milliseconds(BatchDuration))
  }

  protected def stopStreamingContext() {
    ssc.stop()
  }

}
