# It's a Spark application to parse the data from wikipedia dump and run the pagerank

## How to use

1. `sbt package` to generate the jar

2. Download the Mahout Integration Jar from [here](http://mvnrepository.com/artifact/org.apache.mahout/mahout-integration). Make sure the version are match to the one in `build.sbt`. Mahout Integration is the only jar needed when running.

3. Run the class on `Spark bin/spark-submit --master spark://master:7077 --jars ./path/to/downloaded/jar --class ParseWiki ./path/to/build/jar /path/to/xml/on/hdfd slice_number`
