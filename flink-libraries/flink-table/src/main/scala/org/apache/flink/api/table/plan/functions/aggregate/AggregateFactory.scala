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

import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.sql.fun.{SqlAvgAggFunction, SqlSumAggFunction}
import org.apache.flink.api.common.functions.RichGroupReduceFunction
import org.apache.flink.api.table.plan.OptimizeException
import org.apache.flink.api.table.plan.functions.AggregateFunction

object AggregateFactory {

  def createAggregateInstance(aggregateCalls: Seq[AggregateCall]): 
    RichGroupReduceFunction[Any, Any] = {
    
    val fieldIndexes = new Array[Int](aggregateCalls.size)
    val aggregates = new Array[Aggregate[_ <: Any]](aggregateCalls.size)
    aggregateCalls.zipWithIndex.map { case (aggregateCall, index) =>
      val sqlType = aggregateCall.getType
      // currently assume only aggregate on singleton field.
      val fieldIndex = aggregateCall.getArgList.get(0);
      fieldIndexes(index) = fieldIndex
      aggregateCall.getAggregation match {
        case sqlAggFunction: SqlSumAggFunction => {
          sqlType.getSqlTypeName.getName match {
            case "INTEGER" =>
              aggregates(index) = new IntSumAggregate
            case "BIGINT" =>
              aggregates(index) = new LongSumAggregate
            case "FLOAT" =>
              aggregates(index) = new FloatSumAggregate
            case "DOUBLE" =>
              aggregates(index) = new DoubleSumAggregate
            case sqlType: String =>
              throw new OptimizeException("Sum aggregate does no support type:" + sqlType)
          }
        }
        case sqlAvgFunction: SqlAvgAggFunction => {
          sqlType.getSqlTypeName.getName match {
            case "INTEGER" =>
              aggregates(index) = new IntAvgAggregate
            case "BIGINT" =>
              aggregates(index) = new LongAvgAggregate
            case "FLOAT" =>
              aggregates(index) = new FloatAvgAggregate
            case "DOUBLE" =>
              aggregates(index) = new DoubleAvgAggregate
            case sqlType: String => 
              throw new OptimizeException("Avg aggregate does no support type:" + sqlType)  
          }
        }  
      }
    }

    new AggregateFunction(aggregates, fieldIndexes)
  }

}
