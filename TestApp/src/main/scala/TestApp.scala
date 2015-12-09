import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner

object TestApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Test App")
    val sc = new SparkContext(conf)
    // val data = sc.parallelize(1 to 100, 4)
    // val b = data.filter(x => x < 50)
    // val c = b.groupBy(x => x % 4)
    // c.collect
    // val file = sc.textFile("file:///home/frankfzw/wikipedia/wiki.txt")
    // val counts = file.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    // counts.saveAsTextFile("file:///home/frankfzw/wikipedia/res")

    val data1 = Array[(Int, Char)](
    (1, 'a'), (2, 'b'),
    (3, 'c'), (4, 'd'),
    (5, 'e'), (3, 'f'),
    (2, 'g'), (1, 'h'))
    val rangePairs1 = sc.parallelize(data1, 3)

    val hashPairs1 = rangePairs1.partitionBy(new HashPartitioner(3))


    val data2 = Array[(Int, String)]((1, "A"), (2, "B"),
      (3, "C"), (4, "D"))

    val pairs2 = sc.parallelize(data2, 2)
    val rangePairs2 = pairs2.map(x => (x._1, x._2.charAt(0)))


    val data3 = Array[(Int, Char)]((1, 'X'), (2, 'Y'), (5, 'Z'))
    val rangePairs3 = sc.parallelize(data3, 2)


    val rangePairs = rangePairs2.union(rangePairs3)


    val result = hashPairs1.join(rangePairs)

    result.foreach(println)

  }
}
