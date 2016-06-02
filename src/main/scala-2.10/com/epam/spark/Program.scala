package com.epam.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AvgBytesCounter {

  val appName:String = "Server Log Analuzer"
  val master:String = "local"
  var sc:SparkContext = _

  def main(args: Array[String]) {
    if (args.length != 2){
      print("Please use 2 parameters: [source file, dest file]")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName(appName).setMaster(master)
    sc = new SparkContext(conf)

    val dataset = sc.textFile(args(0))
    val topLogs = getTop5(dataset)
    topLogs.saveAsTextFile("hdfs://"+ args(1))

  }

  def getTop5(text: RDD[String]): RDD[String]={
    val logs = text.map(s=>s.split("""(?:[\"\]\[]|[\]] [\"\]\[]|[\]])|(?:( - - ))"""))
      .filter(f=>(f(0).length>2 && f(5).split(" ").size==3))
      .map(line => (parseIp(line(0)), parseBytes(line(5))))
      .groupByKey
      .map{case (ip, bytes) =>(ip, arrStats(bytes))}
      .map{case (k, stats) =>(stats._2, Tuple2(k, stats._1))}
      .sortByKey(false,1)
      .map{case (key, value) =>( value._1.toString +";"+key.toString +";"+ value._2.toString)}

    return sc.parallelize(logs.take(5)).coalesce(1)
  }


  def arrStats(arr: Iterable[Int]):(Int, Int)={

    var totalBytes:Int = 0
    var counter:Int = 0
    arr.foreach(elem =>
    {
      totalBytes+=elem.toInt
      counter+=1
    })
    return(totalBytes, totalBytes/counter)
  }

  def parseBytes(str:String):Int={
    val elem:String = str.split(" ").apply(2)
    if (elem.equals("-"))
      return 0
    return elem.toInt
  }
  def parseIp(str:String):Int={
    val elem:String = str.substring(2, str.length)
    return elem.toInt
  }
}

