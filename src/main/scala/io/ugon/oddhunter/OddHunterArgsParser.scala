package io.ugon.oddhunter

import scala.annotation.tailrec

trait OddHunterArgsParser {

  protected val InputArgKey = "--input"
  protected val OutputArgKey = "--output"
  protected val AWSProfileArgKey = "--aws-profile"
  protected val AWSUseDefaultProviderChainArgKey = "--aws-use-default-provider-chain"
  protected val JobJarArgKey = "--job-jar"
  protected val JobFlowIdArgKey = "--job-flow-id"
  protected val S3JarLocation = "--s3-jar-location"
  protected val S3Bucket = "--s3-bucket"
  protected val S3JarFile = "--s3-jar-file"

  protected def parseArgs(args: Array[String]): Map[String, String] = {
    @tailrec
    def doParseArgs(acc: Map[String, String], remaining: List[String]): Map[String, String] = remaining match {
      case InputArgKey :: value :: tail => doParseArgs(acc + (InputArgKey -> value), tail)
      case OutputArgKey :: value :: tail => doParseArgs(acc + (OutputArgKey -> value), tail)
      case JobJarArgKey :: value :: tail => doParseArgs(acc + (JobJarArgKey -> value), tail)
      case AWSProfileArgKey :: value :: tail => doParseArgs(acc + (AWSProfileArgKey -> value), tail)
      case JobFlowIdArgKey :: value :: tail => doParseArgs(acc + (JobFlowIdArgKey -> value), tail)
      case S3JarLocation :: value :: tail => doParseArgs(acc + (S3JarLocation -> value), tail)
      case S3Bucket :: value :: tail => doParseArgs(acc + (S3Bucket -> value), tail)
      case S3JarFile :: value :: tail => doParseArgs(acc + (S3JarFile -> value), tail)
      case AWSUseDefaultProviderChainArgKey :: tail => doParseArgs(acc + (AWSUseDefaultProviderChainArgKey -> ""), tail)
      case Nil => acc
      case _ :: tail => doParseArgs(acc, tail)
    }

    doParseArgs(Map.empty, args.toList)
  }

}
