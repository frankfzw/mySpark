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

import java.nio.ByteBuffer

import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.storage.{BlockManager, BlockManagerInfo, BlockManagerId}

import scala.language.existentials

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.ShuffleWriter
import scala.collection.mutable.HashMap

/**
* A ShuffleMapTask divides the elements of an RDD into multiple buckets (based on a partitioner
* specified in the ShuffleDependency).
*
* See [[org.apache.spark.scheduler.Task]] for more information.
*
 * @param stageId id of the stage this task belongs to
 * @param taskBinary broadcast version of the RDD and the ShuffleDependency. Once deserialized,
 *                   the type should be (RDD[_], ShuffleDependency[_, _, _]).
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 */
private[spark] class ShuffleMapTask(
    stageId: Int,
    stageAttemptId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    @transient private var locs: Seq[TaskLocation],
    internalAccumulators: Seq[Accumulator[Long]])
  extends Task[MapStatus](stageId, stageAttemptId, partition.index, internalAccumulators)
  with Logging {

  /**
   * added by frankfzw
   * It's more straightforward way to get the corresponding shuffleId without deserialize the task binary
   */
  private var shuffleId: Int = -1

  /**
   * added by frankfzw
   * It's the flag of whether it should pipe Shuffle
   */
  private var pipeFlag: Boolean = false
  // private var reduceStatuses: Array[ReduceStatus] = null
  private var targetBlockManger: HashMap[Int, RpcEndpointRef] = null

  private var executorId: String = null

  def setShuffleId(sId: Int): Unit = {
    shuffleId = sId
  }

  def getShuffleId(): Int = {
    shuffleId
  }

  def getName(): String = {
    this.getClass.getName
  }

  def setPipeFlag(pIdToBlockManager: HashMap[Int, RpcEndpointRef], eId: String): Unit = {
    pipeFlag = true
    targetBlockManger = new HashMap[Int, RpcEndpointRef]()
    targetBlockManger = pIdToBlockManager
    executorId = eId
    // targetBlockManger.foreach(kv => logInfo(s"frankfzw: Target BlockManger ${kv._1} : ${kv._2.slaveEndpoint.address}"))
  }

  /** A constructor used only in test suites. This does not require passing in an RDD. */
  def this(partitionId: Int) {
    this(0, 0, null, new Partition { override def index: Int = 0 }, null, null)
  }

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  override def runTask(context: TaskContext): MapStatus = {
    // Deserialize the RDD using the broadcast variable.
    val deserializeStartTime = System.currentTimeMillis()
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime

    metrics = Some(context.taskMetrics)
    var writer: ShuffleWriter[Any, Any] = null
    try {
      val manager = SparkEnv.get.shuffleManager
      writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)
      // logInfo(s"frankfzw: wirter class is ${writer.getClass.getName}")
      // writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      // if (pipeFlag) {
      //   // targetBlockManger.foreach(kv => logInfo(s"frankfzw: Target BlockManger ${kv._1} : ${kv._2.slaveEndpoint.address}"))
      //   for (kv <- targetBlockManger) {
      //     BlockManager.pipeStart(kv._2, shuffleId, partitionId, executorId, kv._1)
      //   }
      //   writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      // } else {
      //   writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      // }
      writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      val ret = writer.stop(success = true).get
      // for (kv <- targetBlockManger) {
      //   BlockManager.pipeEnd(kv._2, shuffleId, partitionId, kv._1, ret.getSizeForBlock(kv._1))
      // }
      ret
    } catch {
      case e: Exception =>
        try {
          if (writer != null) {
            writer.stop(success = false)
          }
        } catch {
          case e: Exception =>
            log.debug("Could not stop writer", e)
        }
        throw e
    }
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "ShuffleMapTask(%d, %d)".format(stageId, partitionId)

}

private[spark] object ShuffleMapTask {
  def getName(): String = {
    val name = this.getClass.getName
    name.substring(0, name.length - 1)
  }
}
