# Early Scheduling of ResultStage (updated)

## Stage
add `val PENDING = false`. The default status of a stage is false. When it's submitted with unfinished parents, we set `PENDING = true`

## DAGScheduler
### submitStage: Important
Submit the satge from the **Final stage to their parents**, i.e. we find the final stage, mark it `PENDING` if it has missing parents, and submit it, and then find it's parents and submit them recursively.

### submitMissingTasks: 
If the stage is `PENDING`, we use `getRandomLocs` to assign the tasks to the random locations.

After that, we find the shuffle dependency on the border of the stage and call `BlockManger.registerShufflePipe` to notify the random reducer to get ready. Besides it calls the `MapOutputTracker.registerPendingReduce` to register shuffle pipe.

### getRandomLocs(new added): 
Ask the `BlockManagerMaster` to get the acitve block host and assign the task to the locations.

## BlockManagerMaster
### getBlockManagerList(): 
Return a Seq of `BlockManagerId` to the `DAGScheduler`.

It calls the `BlockManagerMasterEndpoint` to fetch the whole list of `BlockManagerId`

## BlockManagerMasterEndpoint
Add a case which is `GetBlockMangerList`. It calls the function `getBlockManagerList()`
### getBlockMangerList(): 
Return the `blockMangerInfo.keySet` which is a collection of `BlockManagerId`


## 
    These are for the early scheduling of stage and its tasks.

    Now when we start the tasks, we have to make the executor waiting until the ShuffleStage finishes.

    Since the RDD of the early scheduling stage will always be a ShuffledRDD, we modified the ShuffledRDD
    to make it wait.

## BlockStoreShuffleReader
It calls `MapOutputTracker` to fill the `ShuffleBlockFetcherIterator` in read
### read:
Modify the function, if the `blockFetcherItr` is empty, return the `null`

## ShuffleBlockFetcherIterator
### empty(new added): 
Return `true` if it didn't get the `blocksByAddress` from `MapOutputTracker`. 

## MapOutputTracker
### getMapSizesByExecutorId: 
It's called by `BlockStoreShuffleReader`. It than calls `convertMapStatuses` to get the `BlockManagerId` and the corresponding `BlockId` 
### covertMapStatuses: 
If we get a `null` status which means the map is unfinished, we return a empty Seq


# Change the data transmission from reducer-fetching to mapper-pushing

## Register the reduce status

Create a new data structure `ReduceStatus`, which contains the `partitionId` and `BlockManagerId` to store the statuses of reducers

### DAGScheduler
#### submitMissingTasks
if it's a PENDING stage, find it's dependency and call `MapOutputTracker.registerPendingReduce` to register the shuffleId with an Array of `ReduceStatus`

### ShuffleMapTask
Add a parameter named `shuffleId` to record if this task has an shuffle to complete. Add the corresponding set and get functions as well.
Add a parameter named `pipeFlag` to define whether the results should be pushed to the remote BlockManager
Add a parameter named `targetBlockManager: HashMap[Int, BlockManagerInfo]` to store the reduceId and corresponding BlockManagerInfo which contains a remote `BlockManagerSlaveEndPoint rpcRef`. The data struct could be Seq or Array to save memory?
Add a function named `getName()` in object of ShuffleMapTask to get the class name of the ShuffleMapTask
Add a function named `setPipeFlag(pidToBlockManager: HashMap[Int, BlockManagerInfo])` to set the pipeFlag and targetBlockManager


### MapOutputTracker
Add a HashMap `reduceStatuses` to store the `ReduceStatus` and `ShuffleId`, a HashMap `cachedReduceStatuses` to store the serialized `reduceStatuses`
#### registerPendingReduce(new added)
Register the reduce by adding an entry in `reduceStatuses`
#### GetReduceStatus(shuffleId: Int) (new added)
This is a RPC message for endhost to get the `reduceStatuses`. When the `MapOutputTrackerEndpoint` receives this message, it will call `MapOutputTrackerMaster.getSerializedReduceStatuses(shufflId)`
#### MapOutputTrackerMaster.getSerializedReduceStatuses(shufflId) (new added)
It first check the cachedReduceStatuses. If the check fails, it than check `reduceStatuses` and serialize the statuses
#### serializeReduceStatuses
Serialze the array of `ReduceStatus` to the array of `Byte`
#### deserializeReduceStatuses
Do the opposite thing of `serializeReduceStatuses`
#### getReduceStatuses(shuffleId: Int) (new added)
It's called by `Exector` to fetch the corresponding `ReduceStatuses`

## Perform data pushing

### DAGScheduler
#### submitMissingTasks()
If the PENDING stage has shuffle dependency, `registerShufflePipe` with the shuffle id and their locations which are randomly allocated by scheduler.
It calls the BlockManager.registerShufflePipe

### BlockManager
Since there may be several tasks running on one BlockManger, the data structures to cache the status and the map data are complicated.

#### `private val shuffleDataCache: HashMap[Int, Array[ArrayBuffer[(Any, Any)]]]`
It's used to store the map data, the key is the shuffleId, the value is an Array indexed by the reduce partition number. Each member of an `Array` is an `ArrayBuffer` to store the map data for the reduce partition. Since there may more than one `ReduceTask`s running on the BlockManager, we have to store the each reduce partition by seperately `ArrayBuffer` 

#### `private val shuffleCacheStatus: HashMap[Int, Array[CountDownLatch]]`
It's used to record the map condition of each reduce partition. The key is the shuffle ID, the value is the `Array` of 	`CountDownLatch` indexed by the reduce partition number.
It maybe changed in the future to make it more effective.

#### registerShufflePipe(new added) in Object
It calls the RegisterShufflePipe of each rpc ref of BlockManager to register the shuffle with the shuffle id, total map partition number, the total reduce partition number and the reduce partition that belongs to this task.

#### registerShufflePipe(new added) in Class
It creates a key value pair in `shuffleCacheStatus` with `shuffleId -> Array[CountDownLatch]` with the number of total reduce partitions. Each `CountDownLatch` has the number of map patitions to count down.

Besides, it create the the the cache with `shuffleId -> Array[ArrayBuffer[(Any, Any)]]` with the number of reduce partitions.

#### getRemoteBlockManager(new added)
It's called by `Exectutor` to fetch the remote rpcRef of `BlockManagerSlaveEndpoint`

It ask the `BlockManagerMaster` to fetch the rpcRef

#### writeRemote(new added)
Send the key-value pair via rpcRef to the remote `BlockManager`

#### pipeStart(new added)
It's used for debuging

#### pipeEnd(new added)
It's used to count down the map number of the waitting reduce partition of one shuffle

#### isCached(new added)
It's called by `BlockStoreShuffleReader` to find out the whether the reduce data of a `shuffleId` is cached.

#### getCache(new added)
It's called by `BlockStoreShuffleReader`. It returns a `Future[ArrayBuffer[(Any, Any)]]` with the waiting of  `CountDownLatch` which contains the stream of key-value pairs.

### BlockManagerMaster
#### getRemoteBlockManager(new added)
It's called by `BlockManager.getRemoteBlockManager`.
It calls the `BlockManagerMasterEndpoint` to fetch the `BlockManagerInfo`

### BlockManagerMessages
#### AskForRemoteBlockManager(new added)
It's a rpc message to get the remote `BlockManagerInfo` by transmit the `BlockManagerId` to the `BlockManagerMasterEndpoint`

#### WriteRemote(new added)
It's a rpc message to write the key-value pair to the remote `BlockManager`

#### RegisterShufflePipe(new added)
It's a rpc message to call `registerShufflePipe`

#### PipeStart(new added)
It's a rpc message to call `pipeStart`

#### PipeEnd(new added)
It's a rpc message to call `pipeEnd`

### BlockManagerMasterEndpoint
#### receiveAndReply
Add a new case which is `AskForRemoteBlockManager`. It replies by calling `getRemoteBlockManager`

#### getRemoteBlockManager(new added)
Returns the corresponding `BlockManagerInfo`


### Exector
#### run
It checks the shuffleId of the task. Ask the `MapOutputTrackerMaster` to fetch the array of `ReduceStatuses`

If the return value of `reduceStatuses` is not null, which means there are some tasks waiting for this one, it calls the `setPipeFlag` to make the task push data.

### ShuffleMapTask
#### runTask
If the `pipeFlag` is true, perform `writeRemote` instead of write. 

The `writer` here could be `SortShuffleWriter`, `HashShuffleWriter` or `UnsafeShuffleWriter`. These three writer all extend the `ShuffleWriter`. We skip the `UnsafeShuffleWriter` at first.

### ShuffleWriter
Add an interface named `writeRemote`

### HashShuffleWriter
#### writeRemote(new added)
It performs like the original `write` except it calls the `BlockManager.writeRemote` to send the data to remote reducer one by one.

### SortShuffleWriter
#### writeRemote(new added)
It performs like the original `write` except it call the `setReduceStatus` to offload the data pushing to the sorter

The sorter could be `ExternalSorter` or `BypassMergeSortShuffleWriter`. They both extends the `SortShuffleFileWriter` which is a Java interface

### SortShuffleFileWriter
Add a method named `setReduceStatus` to set the map with `reduceId` and `BlockManagerInfo`

### ExternalSorter
#### insertAllRemote(new added)
If the `reduceIdToBlockManager` is not null, perform the data pushing without combining or merging

### BypassMergeSortShuffleWriter
#### insertAllRemote(new added)
If the `reduceIdToBlockManager` is not null, perform the data pushing one by one

### BlockStoreShuffleReader
It calls `blockManager.isChached` and `blockManager.getCache` to wait and get the cached data of the corresponding reduce partition of the shuffle.





