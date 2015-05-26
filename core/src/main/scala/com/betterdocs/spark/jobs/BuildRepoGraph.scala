package com.betterdocs.spark.jobs

import spark.jobserver.SparkJob
import spark.jobserver.NamedRddSupport
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import spark.jobserver.SparkJobValidation
import spark.jobserver.SparkJobValid

class BuildRepoGraph extends SparkJob with NamedRddSupport {
  
  def runJob(sc: SparkContext, jobConfig: Config): Any = {
    ???
  }

  def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid
  }
}