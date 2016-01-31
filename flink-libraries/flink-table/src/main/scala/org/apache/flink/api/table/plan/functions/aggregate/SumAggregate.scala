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

abstract class SumAggregate[T] extends Aggregate[T]{

}

class IntSumAggregate extends SumAggregate[Int] {
  
  private var sumValue = 0
  
  override def initiateAggregate: Unit = {
    sumValue = 0
  }


  override def getAggregated(): Int = {
    sumValue
  }

  override def aggregate(value: Iterable[Any]): Unit = {
    sumValue = value.asInstanceOf[Iterable[Int]].sum
  }
}

class LongSumAggregate extends SumAggregate[Long] {

  private var sumValue = 0L

  override def initiateAggregate: Unit = {
    sumValue = 0
  }

  override def aggregate(value: Iterable[Any]): Unit = {
    sumValue = value.asInstanceOf[Iterable[Long]].sum
  }

  override def getAggregated(): Long = {
    sumValue
  }
}

class FloatSumAggregate extends SumAggregate[Float] {
  private var sumValue: Float = 0

  override def initiateAggregate: Unit = {
    sumValue = 0
  }

  override def aggregate(value: Iterable[Any]): Unit = {
    sumValue = value.asInstanceOf[Iterable[Float]].sum
  }

  override def getAggregated(): Float = {
    sumValue
  }
}

class DoubleSumAggregate extends SumAggregate[Double] {
  private var sumValue: Double = 0

  override def initiateAggregate: Unit = {
    sumValue = 0
  }

  override def aggregate(value: Iterable[Any]): Unit = {
    sumValue = value.asInstanceOf[Iterable[Double]].sum
  }

  override def getAggregated(): Double = {
    sumValue
  }
}
