package io.ugon.oddhunter

import org.apache.spark.{SparkConf, SparkContext}

object OddHunterEmrSolver extends OddHunterSolver with Serializable {

  final def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Odd Hunter")
    val sparkContext = new SparkContext(conf)
    solve(sparkContext, args(0), args(1))
  }

}
