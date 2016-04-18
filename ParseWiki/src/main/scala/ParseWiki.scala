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

/**
 * Created by frankfzw
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable.ArrayBuffer
import java.util.regex.Pattern
import scala.xml.XML
import org.apache.mahout.text.wikipedia._

object ParseWiki {

  var SLICES: Int = 100
  var PATH_TO_SAVE = ""
  var ITER: Int = 10

  class Article(raw: String) extends Serializable{
    val links: Array[Long] = Article.parseLink(raw).distinct
    val redirect: Boolean = !Article.redirectPattern.findFirstIn(raw).isEmpty
    val stub: Boolean = !Article.stubPattern.findFirstIn(raw).isEmpty
    val disambig: Boolean = !Article.disambigPattern.findFirstIn(raw).isEmpty
    val tiXML = Article.titlePattern.findFirstIn(raw).getOrElse("")
    val title: String = {
      try {
        XML.loadString(tiXML).text
      } catch {
        case e => Article.notFoundString
      }
    }
    val relevant: Boolean = !(redirect || stub || disambig || title == Article.notFoundString || title == null)

    override def toString: String = {
      var linksStr = ""
      for (link <- links) {
        linksStr += link
        linksStr += ","
      }
      title + " " + linksStr
    }

  }

  object Article {
    val titlePattern = "<title>(.*)<\\/title>".r
    val redirectPattern = "#REDIRECT\\s+\\[\\[(.*?)\\]\\]".r
    val disambigPattern = "\\{\\{disambig\\}\\}".r
    val stubPattern = "\\-stub\\}\\}".r
    val linkPattern = Pattern.compile("\\[\\[(.*?)\\]\\]", Pattern.MULTILINE)

    val notFoundString = "NOTFOUND"

    def parseLink(line: String): Array[String] = {
      val res = new ArrayBuffer[String]()
      val matcher = linkPattern.matcher(line)
      while (matcher.find()) {
        val temp: Array[String] = matcher.group(1).split("\\|")
        if (temp != null && temp.length > 0) {
          val link = temp(0)
          //filter the File:
          if (!link.contains(":")) {
            res += link
          }
        }
      }
      res.toArray
    }

    def formatTitle(title: String): String = {
      title.trim.toLowerCase.replace(" ", "_")
    }

    def hashTitle(formatted: String): Long = {
      var h: Long = 1125899906842597L
      val len = formatted.length
      for (i <- 0 until len) {
        h = 31*h + formatted.charAt(i)
      }
      h
    }

  }


  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: ParseWiki <file> <numslice> <path_to_save>")
      System.exit(1)
    }

    SLICES = args(1).toInt
    PATH_TO_SAVE = args(2)
    ITER = args(3).toInt

    //load xml from hdfs
    val conf = new Configuration
    conf.set("key.value.separator.in.input.line", " ")
    conf.set("xmlinput.start", "<page>")
    conf.set("xmlinput.end", "</page>")

    val sparkConf = new SparkConf().setAppName("ParseWiki")
    val sc = new SparkContext(sparkConf)

    val xmlRDD = sc.newAPIHadoopFile(args(0), classOf[XmlInputFormat], classOf[LongWritable], classOf[Text], conf)
      .map(t => t._2.toString).coalesce(SLICES, true)
    val allArtsRDD = xmlRDD.map { raw => new Article(raw) }
    //cache the articles
    val graph = allArtsRDD.filter(a => a.relevant).cache()
    println(graph.count)
    val links = graph.map(art => (art.title, art.links))
    val result = links.flatMap{ case(title, links) =>
      links.map(link => (s"${Article.hashTitle(Article.formatTitle(title))} ${Article.hashTitle(Article.formatTitle(link))}"))
    }
    result.repartition(1).saveAsTextFile(PATH_TO_SAVE)

    // var ranks = links.mapValues(v => 1.0)

    // for (i <- 1 to ITER) {
    //   val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
    //     val size = urls.size
    //     urls.map(url => (url, rank / size))
    //   }
    //   ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    // }

    // val output = ranks.collect()
    // output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))

    // graph.saveAsTextFile(PATH_TO_SAVE)

  }
}
