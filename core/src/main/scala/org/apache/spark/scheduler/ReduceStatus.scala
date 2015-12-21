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

package org.apache.spark.scheduler

import java.io.{ObjectInput, ObjectOutput, Externalizable}

import org.apache.spark.util.Utils
import org.apache.spark.storage.BlockManagerId
/**
 * Created by frankfzw on 15-11-9.
 */
private[spark] class ReduceStatus(var partition: Int, var executorId: String) extends Externalizable{

  private var totalMapPartition:Int = 0;

  protected def this() = this(0, null)

  override def readExternal(in: ObjectInput): Unit = {
    partition = in.readInt()
    executorId = in.readUTF()
    totalMapPartition = in.readInt()
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeInt(partition)
    out.writeUTF(executorId)
    out.writeInt(totalMapPartition)
  }

  def setTotalMapPartition(num: Int): Unit = {
    totalMapPartition = num
  }

  def getTotalMapPartiton(): Int = totalMapPartition

}
