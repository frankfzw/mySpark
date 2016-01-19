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

package org.apache.spark.shuffle

import java.util.concurrent.{LinkedBlockingQueue, CountDownLatch}

import org.apache.hadoop.io.SequenceFile.Sorter.RawKeyValueIterator
import org.apache.spark._
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage._
import org.apache.spark.util.{NextIterator, CompletionIterator}
import org.apache.spark.util.collection.ExternalSorter

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Future, Await}
import scala.concurrent.duration.Duration
import scala.util.control.Breaks._

/**
 * Fetches and reads the partitions in range [startPartition, endPartition) from a shuffle by
 * requesting them from other nodes' block stores.
 */
private[spark] class BlockStoreShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
  extends ShuffleReader[K, C] with Logging {

  private val dep = handle.dependency

  /** Read the combined key-values for this reduce task */
  override def read(): Iterator[Product2[K, C]] = {
    // added by frankfzw: check if the data is pushed by the mapper at first
    var recordIter: Iterator[(Any, Any)] = null
    val ser = Serializer.getSerializer(dep.serializer)
    val serializerInstance = ser.newInstance()
    var fromCache: Boolean = false

    var blockFetcherItr: ShuffleBlockFetcherIterator = null
    if (blockManager.isCached(handle.shuffleId, startPartition)) {
      fromCache = true
      // val temp = new mutable.HashMap[BlockManagerId, ArrayBuffer[(BlockId, Long)]]
      val totalMapPartition = blockManager.getMapPartitionNumber(handle.shuffleId)
      val numbers = Array.fill[Int](endPartition - startPartition)(totalMapPartition)
      logInfo(s"frankfzw: Reading from local cache, shuffleId is ${handle.shuffleId}, startPartition: ${startPartition}, endPartition: ${endPartition}, total map partition: ${totalMapPartition}")
      val requests = blockManager.getPendngFetchRequest(handle.shuffleId, startPartition, endPartition)
      blockFetcherItr = new ShuffleBlockFetcherIterator(
      context,
      blockManager.shuffleClient,
      blockManager,
      null,
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024,
      true, requests, numbers)
    } else {
      blockFetcherItr = new ShuffleBlockFetcherIterator(
      context,
      blockManager.shuffleClient,
      blockManager,
      mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition),
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024)

      // frankfzw: check if it get the real data from MapOutputTracker
      if (blockFetcherItr.empty())
        return null
    }
    // Wrap the streams for compression based on configuration
    val wrappedStreams = blockFetcherItr.filter { case (blockId, inputStream) => inputStream != null}.map { case (blockId, inputStream) =>
      blockManager.wrapForCompression(blockId, inputStream)
    }

    // Create a key/value iterator for each stream
    recordIter = wrappedStreams.flatMap { wrappedStream =>
      // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
      // NextIterator. The NextIterator makes sure that close() is called on the
      // underlying InputStream when all records have been read.
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    }
    // Update the context task metrics for each record read.
    val readMetrics = context.taskMetrics.createShuffleReadMetricsForDependency()
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map(record => {
        readMetrics.incRecordsRead(1)
        record
      }),
      context.taskMetrics().updateShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine && !fromCache) {
        // We are reading values that are already combined
        // added by frankfzw: make sure these key-value pairs are not read from local cache
        logInfo("frankfzw: This shuffle has combined by mapper and it doesn't readed from cache")
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        logInfo("frankfzw: This shuffle hasn't be combined or it's from cache")
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // while (aggregatedIter.hasNext) {
    //   val kv = aggregatedIter.next()
    //   logInfo(s"frankfzw: The kv is ${kv._1} -> ${kv._2}")
    // }
    // Sort the output if there is a sort ordering defined.
    dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        logInfo("frankfzw: It has external order")
        // Create an ExternalSorter to sort the data. Note that if spark.shuffle.spill is disabled,
        // the ExternalSorter won't spill to disk.
        val sorter = new ExternalSorter[K, C, C](ordering = Some(keyOrd), serializer = Some(ser))
        // shouldn't pass the shuffleId since we don'nt
        sorter.insertAll(aggregatedIter)
        context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
        context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
        context.internalMetricsToAccumulators(
          InternalAccumulator.PEAK_EXECUTION_MEMORY).add(sorter.peakMemoryUsedBytes)
        sorter.iterator
      case None =>
        logInfo("frankfzw: It doesn't have external order")
        // while (recordIter.hasNext) {
        //   val kv = recordIter.next()
        //   logInfo(s"frankfzw: The kv is ${kv._1} -> ${kv._2}")
        // }
        aggregatedIter
    }

  }
}
