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

package org.apache.flink.api.table.plan

import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.core.JoinRelType._
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.operators.join.JoinType

object TypeConverter {

  def typeInfoToSqlType(typeInfo: TypeInformation[_]): SqlTypeName = typeInfo match {
    case BOOLEAN_TYPE_INFO => BOOLEAN
    case BYTE_TYPE_INFO => TINYINT
    case SHORT_TYPE_INFO => SMALLINT
    case INT_TYPE_INFO => INTEGER
    case LONG_TYPE_INFO => BIGINT
    case FLOAT_TYPE_INFO => FLOAT
    case DOUBLE_TYPE_INFO => DOUBLE
    case STRING_TYPE_INFO => VARCHAR
    case DATE_TYPE_INFO => DATE
    case _ => ??? // TODO more types
    }

  def sqlTypeToTypeInfo(sqlType: SqlTypeName): TypeInformation[_] = sqlType match {
    case BOOLEAN => BOOLEAN_TYPE_INFO
    case TINYINT => BYTE_TYPE_INFO
    case SMALLINT => SHORT_TYPE_INFO
    case INTEGER => INT_TYPE_INFO
    case BIGINT => LONG_TYPE_INFO
    case FLOAT => FLOAT_TYPE_INFO
    case DOUBLE => DOUBLE_TYPE_INFO
    case VARCHAR | CHAR => STRING_TYPE_INFO
    case DATE => DATE_TYPE_INFO
    case _ => ??? // TODO more types
    }

  def sqlJoinTypeToFlinkJoinType(sqlJoinType: JoinRelType): JoinType = sqlJoinType match {
    case INNER => JoinType.INNER
    case LEFT => JoinType.LEFT_OUTER
    case RIGHT => JoinType.RIGHT_OUTER
    case FULL => JoinType.FULL_OUTER
  }
}