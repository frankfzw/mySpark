##Stage
    add
```scala 
val PENDING = false
```
    The default status of a stage is false.
    When it's submitted with unfinished parents, we set PENDING = true;

##DAGScheduler
    ###submitStage: if (missing.isEmpty), this stage will become running. So we check every stage in waitingStages, if their parents are all running, we try to submit the stage and mark PENDING = true;
    ###submitMissingTasks: if the stage is PENDING, we use getRandomLocs to assign the tasks to the random locations.
    ###getRandomLocs(new added): ask the BlockManagerMaster to get the acitve block host and assign the task to the locations.

##BlockManagerMaster
    ###getBlockManagerList(): return a Seq of BlockManagerId to the DAGScheduler.
                           It calls the BlockManagerMasterEndpoint to fetch the whole list of BlockManagerId

##BlockManagerMasterEndpoint
    add a case which is GetBlockMangerList. It calls the function getBlockManagerList()
    ###getBlockMangerList(): return the blockMangerInfo.keySet which is a collection of BlockManagerId

These are for the early scheduling of stage and its tasks.

Now when we start the tasks, we have to make the executor waiting until the ShuffleStage finishes.

Since the RDD of the early scheduling stage will always be a ShuffledRDD, we modified the ShuffledRDD to make it wait.

##ShuffledRDD
    ###compute: sleep 50 ms and do check until the read doesn't return null

##BlockStoreShuffleReader
    It call mapOutputTracker to fill the ShuffleBlockFetcherIterator in read
    ###read: modify the function, if the blockFetcherItr is empty, return the null

##ShuffleBlockFetcherIterator
    ###empty(new added): return true if it didn't get the blocksByAddress from MapOutputTracker. 

##MapOutputTracker
    ###getMapSizesByExecutorId: called by BlockStoreShuffleReader, call convertMapStatuses to get the BlockManagerId and the corresponding BlockId 
    ###covertMapStatuses: if we get a null status which means the map is unfinished, we return a empty Seq
