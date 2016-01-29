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

import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.sql.fun.SqlCountAggFunction
import org.apache.flink.api.common.functions.RichGroupReduceFunction
import org.apache.flink.api.table.Row

import scala.collection.mutable.ArrayBuffer

object AggregateFactory {

  def createAggregateInstance(aggregateCalls: Seq[AggregateCall]): RichGroupReduceFunction[Row, Row] = {
    val fieldIndexes = new ArrayBuffer[Int]
    val aggregates = new ArrayBuffer[Aggregate]
    aggregateCalls.map { aggregateCall =>
      val sqlType = aggregateCall.getType
      // currently assume only aggregate on singleton field.
      val fieldIndex = aggregateCall.getArgList.get(0);
      fieldIndexes += fieldIndex
      aggregateCall.getAggregation match {
        case SqlCountAggFunction =>
          sqlType.getSqlTypeName match {
            case "BIGINT" =>
              aggregates += new LongSumAggregate
          }
      }
    }

    new AggregateFunction(aggregates.toArray, fieldIndexes.toArray)
  }

}
