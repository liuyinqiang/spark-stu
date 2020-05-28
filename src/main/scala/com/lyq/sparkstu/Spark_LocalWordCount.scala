package com.lyq.sparkstu

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Spark_LocalWordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("LocalWordCount")
    val sc = SparkContext.getOrCreate(config = sparkConf)
//    ss = SparkSession.builder().master("local[*]").appName("LocalWordCount")

    val textfile = sc.textFile("D:\\A_tmp\\file\\README.md")
    val aggregatefile = textfile.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    aggregatefile.foreach(println)

  }
}
