/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.api.table.plan.functions.aggregate

abstract class AvgAggregate[T] extends Aggregate[T] {

}

class IntAvgAggregate extends AvgAggregate[Int] {
  private var avgValue: Int = 0
  
  override def initiateAggregate: Unit = {
    avgValue = 0
  }

  override def aggregate(value: Iterable[Any]): Unit = {
    avgValue = value.asInstanceOf[Iterable[Int]].sum / value.size
  }

  override def getAggregated(): Int = {
    avgValue
  }
}

class LongAvgAggregate extends AvgAggregate[Long] {
  private var avgValue: Long = 0

  override def initiateAggregate: Unit = {
    avgValue = 0
  }

  override def aggregate(value: Iterable[Any]): Unit = {
    avgValue = value.asInstanceOf[Iterable[Long]].sum / value.size
  }

  override def getAggregated(): Long = {
    avgValue
  }
}

class FloatAvgAggregate extends AvgAggregate[Float] {
  private var avgValue: Float = 0

  override def initiateAggregate: Unit = {
    avgValue = 0
  }

  override def aggregate(value: Iterable[Any]): Unit = {
    avgValue = value.asInstanceOf[Iterable[Float]].sum / value.size
  }

  override def getAggregated(): Float = {
    avgValue
  }
}

class DoubleAvgAggregate extends AvgAggregate[Double] {
  private var avgValue: Double = 0

  override def initiateAggregate: Unit = {
    avgValue = 0
  }

  override def aggregate(value: Iterable[Any]): Unit = {
    avgValue = value.asInstanceOf[Iterable[Double]].sum / value.size
  }

  override def getAggregated(): Double = {
    avgValue
  }
}
