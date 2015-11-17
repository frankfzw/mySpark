# Early Scheduling of ResultStage

## Stage
add `val PENDING = false`. The default status of a stage is false. When it's submitted with unfinished parents, we set `PENDING = true`

## DAGScheduler
### submitStage: 
if `(missing.isEmpty)`, this stage will become running. So we check every stage in waitingStages, 

if their parents are all running, we try to submit the stage and mark `PENDING = true`;
### submitMissingTasks: 
if the stage is `PENDING`, we use getRandomLocs to assign the tasks to the random locations.
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

## ShuffledRDD
### compute: 
Sleep 50 ms and do check until the read doesn't return null
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
Add an parameter named `shuffleId` to record if this task has an shuffle to complete

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

### Exector
It checks the shuffleId of the task. Ask the `MapOutputTrackerMaster` to fetch the array of `ReduceStatuses`

