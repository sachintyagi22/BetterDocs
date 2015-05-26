/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.betterdocs.spark

import com.betterdocs.configuration.BetterDocsConfig
import com.betterdocs.crawler.Repository
import com.betterdocs.parser.MethodVisitorHelper._
import com.betterdocs.spark.SparkIndexJobHelper._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object CreateIndexJobForMethods {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster(BetterDocsConfig.sparkMaster)
      .setAppName("CreateIndexJobForMethods")
      .set("spark.executor.memory", "2g")

    val sc: SparkContext = createSparkContext(conf)

    val zipFileExtractedRDD: RDD[(ArrayBuffer[(String, String)],
      Option[Repository], List[String])] = makeZipFileExtractedRDD(sc)

    // Create indexes for elastic search.
    zipFileExtractedRDD.map { f =>
      val (files, repo, packages) = f
      toJson(extractMethodsAndLineNumbers(files.toMap, repo).toSet, true)
    }.saveAsTextFile(BetterDocsConfig.sparkMethodsOutput)

  }
}
