package io.ugon.oddhunter

import org.apache.spark.SparkContext

trait OddHunterSolver {

  private val CommaOrTabRegex = "[,\t]"

  def solve(spark: SparkContext, inputDir: String, outputDir: String): Unit =
    spark

      // read whole text files so that from each one header can be dropped
      .wholeTextFiles(inputDir)

      // drop headers, create RDD with lines with CSV/TSV pairs
      .flatMap { case (_, content) => content.split("\n").drop(1) }

      // parse each line into key -> value pair, take care of the "empty string" corner case
      // -1 param in split not to drop empty strings
      .map { line =>
        val splitLine = line.split(CommaOrTabRegex, -1).map(number => if (number.isEmpty) 0 else number.toInt)
        splitLine(0) -> splitLine(1)
      }

      // for each key determine the value that occurs odd number of times.
      // XOR will cancel out all even occurrences and for each key leave just the odd result.
      // this transformation should start reducing locally before shuffling the partially reduced data.
      .reduceByKey(_ ^ _)

      // format pairs into TSV
      .map { case (key, value) => s"$key\t$value" }

      // write results to output S3 bucket
      .saveAsTextFile(outputDir)

}
