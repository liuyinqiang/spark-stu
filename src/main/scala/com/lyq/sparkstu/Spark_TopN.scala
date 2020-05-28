package com.lyq.sparkstu

import org.apache.spark.{SparkConf, SparkContext}

object Spark_TopN {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_topN")
    val sc = SparkContext.getOrCreate(config = sparkConf)

    val textfile = sc.textFile("D:\\A_tmp\\file\\subject")
    val topN = textfile.map(line => {
      val idx = line.lastIndexOf("/")
      val teacher = line.substring(idx+1)
      (teacher, 1)
    }).reduceByKey(_+_).sortBy(_._2, false).take(3)

    topN.foreach(println)

  }
}
