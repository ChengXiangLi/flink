package org.apache.flink.api.table.plan.functions

import org.apache.flink.api.table.Row

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
object FunctionUtils {

  def getFieldValue(record: Any, fieldIndex: Int): Any = {
    record match {
      case row: Row => row.productElement(fieldIndex)
      case _ => throw new UnsupportedOperationException("Do not support types other than Row.")
    }
  }
  
  def putFieldValue(record: Any, fieldIndex: Int, fieldValue: Any): Unit = {
    record match {
      case row: Row => row.setField(fieldIndex, fieldValue)
      case _ => throw new UnsupportedOperationException("Do not support types other than Row.")
    }
  }
}
