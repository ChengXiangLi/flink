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
package org.apache.flink.api.table.plan.functions

import java.lang.Iterable

import com.google.common.base.Preconditions
import org.apache.flink.api.common.functions.RichGroupReduceFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._

class AggregateFunction(private val aggregates: Array[Aggregate[_ <: Any]], private val fields: Array[Int])
  extends RichGroupReduceFunction[Any, Any] {

  override def open(config: Configuration) {
    Preconditions.checkNotNull(aggregates)
    Preconditions.checkNotNull(fields)
    Preconditions.checkArgument(aggregates.size == fields.size)

    aggregates.foreach(_.initiateAggregate)
  }

  override def reduce(values: Iterable[Any], out: Collector[Any]): Unit = {
    var currentValue: Any = null

    val aggregateDatas = fields.map(fieldIndex => {
      values.map(value => {
        currentValue = value
        FunctionUtils.getFieldValue(value, fieldIndex)
      })
    })

    aggregates.zip(aggregateDatas).zip(fields).foreach {
      case ((aggregator, aggregateData), fieldIndex) =>
        aggregator.aggregate(aggregateData)
        FunctionUtils.putFieldValue(currentValue, fieldIndex, aggregator.getAggregated())
    }

    out.collect(currentValue)
  }
}
