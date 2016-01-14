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
  

  class Article(raw: String) extends Serializable{
    val links: Array[String] = Article.parseLink(raw).distinct
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

  }


  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: ParseWiki <file> <numslice>")
      System.exit(1)
    }

    if (args.length > 1)
      SLICES = args(1).toInt

    //load xml from hdfs
    val conf = new Configuration
    conf.set("key.value.separator.in.input.line", " ")
    conf.set("xmlinput.start", "<page>")
    conf.set("xmlinput.end", "</page>")

    val sparkConf = new SparkConf().setAppName("ParseWiki")
    val sc = new SparkContext(sparkConf)

    val xmlRDD = sc.newAPIHadoopFile(args(0), classOf[XmlInputFormat], classOf[LongWritable], classOf[Text], conf)
      .map(t => t._2.toString).coalesce(SLICES, false)
    val allArtsRDD = xmlRDD.map { raw => new Article(raw) }
    //cache the articles
    val graph = allArtsRDD.filter(a => a.relevant).cache()

    println(graph.count)

  }
}
