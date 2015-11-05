import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object TestApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Test App")
    val sc = new SparkContext(conf)
    //val data = sc.parallelize(1 to 100, 4)
    //val b = data.filter(x => x < 50)
    //val c = b.groupBy(x => x % 4)
    //c.collect
    val file = sc.textFile("file:///home/frankfzw/wikipedia/wiki.txt")
    val counts = file.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    counts.saveAsTextFile("file:///home/frankfzw/wikipedia/res")
  }
}
