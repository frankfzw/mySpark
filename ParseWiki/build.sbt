name := "ParseWiki"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.0.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"

libraryDependencies += "org.apache.hadoop" %% "hadoop-common" % "2.4.0"

libraryDependencies ++= Seq(
  // Mahout's Spark code
  "commons-io" % "commons-io" % "2.4",
  "org.apache.mahout" % "mahout-math-scala_2.10" % "0.10.0",
  "org.apache.mahout" % "mahout-spark_2.10" % "0.10.0",
  "org.apache.mahout" % "mahout-math" % "0.10.0",
  "org.apache.mahout" % "mahout-hdfs" % "0.10.0",
  // Google collections, AKA Guava
  "com.google.guava" % "guava" % "16.0")

resolvers += "typesafe repo" at " http://repo.typesafe.com/typesafe/releases/"

resolvers += Resolver.mavenLocal
