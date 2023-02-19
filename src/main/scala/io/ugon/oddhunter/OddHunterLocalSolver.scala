package io.ugon.oddhunter

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.auth.{AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import org.apache.spark.sql.SparkSession

object OddHunterLocalSolver extends OddHunterSolver with OddHunterArgsParser with Serializable {

  def main(args: Array[String]): Unit = {
    val parsedArgs = parseArgs(args)

    if (!parsedArgs.contains(InputArgKey)) {
      println(s"Missing $InputArgKey param")
    } else if (!parsedArgs.contains(OutputArgKey)) {
      println(s"Missing $InputArgKey param")
    } else if (!parsedArgs.contains(AWSUseDefaultProviderChainArgKey) && !parsedArgs.contains(AWSProfileArgKey)) {
      println(s"Missing $AWSUseDefaultProviderChainArgKey and $AWSProfileArgKey params - exactly one must be provided")
    } else if (parsedArgs.contains(AWSUseDefaultProviderChainArgKey) && parsedArgs.contains(AWSProfileArgKey)) {
      println(s"Conflicting $AWSUseDefaultProviderChainArgKey and $AWSProfileArgKey params - exactly one must be provided")
    } else {
      val sparkSession = setupSparkSession(parsedArgs.get(AWSProfileArgKey))
      solve(sparkSession.sparkContext, parsedArgs(InputArgKey), parsedArgs(OutputArgKey))
      sparkSession.close()
    }
  }

  private def setupSparkSession(awsProfile: Option[String]): SparkSession = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[4]")
      .appName("oddhunter")
      .getOrCreate()

    val awsCredentialsProvider: AWSCredentialsProvider = awsProfile match {
      case Some(profile) => new ProfileCredentialsProvider(profile)
      case None => new DefaultAWSCredentialsProviderChain()
    }

    val awsCredentials = awsCredentialsProvider.getCredentials
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", awsCredentials.getAWSAccessKeyId)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", awsCredentials.getAWSSecretKey)

    spark
  }


}
