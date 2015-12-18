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

package org.apache.spark.shuffle.hash

import org.apache.spark._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle._
import org.apache.spark.storage.BlockManagerMessages.WriteRemote
import org.apache.spark.storage.{BlockManager, BlockManagerInfo, DiskBlockObjectWriter}
import scala.collection.mutable.HashMap

private[spark] class HashShuffleWriter[K, V](
    shuffleBlockResolver: FileShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, _],
    mapId: Int,
    context: TaskContext)
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency
  private val numOutputSplits = dep.partitioner.numPartitions
  private val metrics = context.taskMetrics

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false

  private val writeMetrics = new ShuffleWriteMetrics()
  metrics.shuffleWriteMetrics = Some(writeMetrics)

  private val blockManager = SparkEnv.get.blockManager
  private val ser = Serializer.getSerializer(dep.serializer.getOrElse(null))
  private val shuffle = shuffleBlockResolver.forMapTask(dep.shuffleId, mapId, numOutputSplits, ser,
    writeMetrics)

  /** Write a bunch of records to this task's output */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    val iter = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        dep.aggregator.get.combineValuesByKey(records, context)
      } else {
        records
      }
    } else {
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
      records
    }

    for (elem <- iter) {
      val bucketId = dep.partitioner.getPartition(elem._1)
      shuffle.writers(bucketId).write(elem._1, elem._2)
    }
  }

  /**
   * added by frankfzw
   * It's kind of ugly
   * Write the key value pair to the memory of remote BlockManager
   * @param records
   * @param reduceIdToBlockManager
   */
  override def writeRemote(records: Iterator[Product2[K, V]], reduceIdToBlockManager: HashMap[Int, RpcEndpointRef]): Unit = {
    val iter = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        dep.aggregator.get.combineValuesByKey(records, context)
      } else {
        records
      }
    } else {
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
      records
    }

    for (elem <- iter) {
      val bucketId = dep.partitioner.getPartition(elem._1)
      shuffle.writers(bucketId).write(elem._1, elem._2)
      val res = reduceIdToBlockManager.get(bucketId) match {
        case Some(info) => BlockManager.writeRemote(info, dep.shuffleId, bucketId, elem._1, elem._2)
        case _ =>
          logInfo(s"frankfzw: No such reducer id ${bucketId}")
      }

      if (res == false) {
        logError(s"frankfzw: Remote write failed with ${bucketId}")
        throw new SparkException(s"frankfzw: Remote write failed with ${bucketId}")
      }
    }
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(initiallySuccess: Boolean): Option[MapStatus] = {
    var success = initiallySuccess
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        try {
          Some(commitWritesAndBuildStatus())
        } catch {
          case e: Exception =>
            logError(s"frankfzw: Something error happens")
            success = false
            revertWrites()
            throw e
        }
      } else {
        revertWrites()
        None
      }
    } finally {
      // Release the writers back to the shuffle block manager.
      if (shuffle != null && shuffle.writers != null) {
        try {
          shuffle.releaseWriters(success)
        } catch {
          case e: Exception => logError("Failed to release shuffle writers", e)
        }
      }
    }
  }

  private def commitWritesAndBuildStatus(): MapStatus = {
    // Commit the writes. Get the size of each bucket block (total block size).
    val sizes: Array[Long] = shuffle.writers.map { writer: DiskBlockObjectWriter =>
      writer.commitAndClose()
      writer.fileSegment().length
    }
    MapStatus(blockManager.shuffleServerId, sizes)
  }

  private def revertWrites(): Unit = {
    if (shuffle != null && shuffle.writers != null) {
      for (writer <- shuffle.writers) {
        writer.revertPartialWritesAndClose()
      }
    }
  }
}
