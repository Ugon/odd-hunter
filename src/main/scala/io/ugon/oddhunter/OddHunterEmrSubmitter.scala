package io.ugon.oddhunter

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.auth.{AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.elasticmapreduce.model.{AddJobFlowStepsRequest, AddJobFlowStepsResult, HadoopJarStepConfig, StepConfig}
import com.amazonaws.services.elasticmapreduce.{AmazonElasticMapReduce, AmazonElasticMapReduceClientBuilder}
import com.amazonaws.services.s3.AmazonS3ClientBuilder

import java.io.File

object OddHunterEmrSubmitter extends OddHunterArgsParser with OddHunterSolver with Serializable {

  def main(args: Array[String]): Unit = {
    val parsedArgs: Map[String, String] = parseArgs(args)

    if (!parsedArgs.contains(InputArgKey)) {
      println(s"Missing $InputArgKey param")
    } else if (!parsedArgs.contains(OutputArgKey)) {
      println(s"Missing $InputArgKey param")
    } else if (!parsedArgs.contains(JobJarArgKey)) {
      println(s"Missing $JobJarArgKey param")
    } else if (!parsedArgs.contains(JobJarArgKey)) {
      println(s"Missing $JobJarArgKey param")
    } else if (!parsedArgs.contains(JobFlowIdArgKey)) {
      println(s"Missing $JobFlowIdArgKey param")
    } else if (!parsedArgs.contains(S3JarLocation)) {
      println(s"Missing $S3JarLocation param")
    } else if (!parsedArgs.contains(S3Bucket)) {
      println(s"Missing $S3Bucket param")
    } else if (!parsedArgs.contains(S3JarFile)) {
      println(s"Missing $S3JarFile param")
    } else if (!parsedArgs.contains(AWSUseDefaultProviderChainArgKey) && !parsedArgs.contains(AWSProfileArgKey)) {
      println(s"Missing $AWSUseDefaultProviderChainArgKey and $AWSProfileArgKey params - exactly one must be provided")
    } else if (parsedArgs.contains(AWSUseDefaultProviderChainArgKey) && parsedArgs.contains(AWSProfileArgKey)) {
      println(s"Conflicting $AWSUseDefaultProviderChainArgKey and $AWSProfileArgKey params - exactly one must be provided")
    } else {
      submitJob(parsedArgs)

    }
  }

  private def submitJob(parsedArgs: Map[String, String]): Unit = {
    val awsCredentialsProvider: AWSCredentialsProvider = parsedArgs.get(AWSProfileArgKey) match {
      case Some(profile) => new ProfileCredentialsProvider(profile)
      case None => new DefaultAWSCredentialsProviderChain()
    }

    // create S3 Client
    val s3Client = AmazonS3ClientBuilder.standard().withCredentials(awsCredentialsProvider).build()

    // upload job jar to S3
    s3Client.putObject(
      parsedArgs(S3Bucket),
      parsedArgs(S3JarFile),
      new File(parsedArgs(JobJarArgKey)))

    // create EMR Client
    val emr: AmazonElasticMapReduce = AmazonElasticMapReduceClientBuilder
      .standard()
      .withCredentials(awsCredentialsProvider)
      .build()

    // prepare step config
    val sparkStepConf: HadoopJarStepConfig = new HadoopJarStepConfig()
      .withJar("command-runner.jar")
      .withArgs(
        "spark-submit",
        "--class", "io.ugon.oddhunter.OddHunterEmrSolver",
        parsedArgs(S3JarLocation),
        parsedArgs(InputArgKey),
        parsedArgs(OutputArgKey)
      )

    val sparkStep: StepConfig = new StepConfig().withName("OddHunter Step")
      .withActionOnFailure("CONTINUE")
      .withHadoopJarStep(sparkStepConf);

    // submit the step to EMR
    val request: AddJobFlowStepsRequest = new AddJobFlowStepsRequest()
      .withJobFlowId(parsedArgs(JobFlowIdArgKey))
      .withSteps(sparkStep)

    val result: AddJobFlowStepsResult = emr.addJobFlowSteps(request)
    println(s"Step ID: ${result.getStepIds}")
  }

}
