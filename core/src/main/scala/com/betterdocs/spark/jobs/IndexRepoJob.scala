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
package com.betterdocs.spark.jobs

import spark.jobserver.SparkJob
import spark.jobserver.NamedRddSupport
import spark.jobserver.SparkJobValidation
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import spark.jobserver.SparkJobValid
import org.apache.spark.rdd.RDD
import java.nio.file.Files
import java.nio.file.Paths
import scala.collection.mutable.ArrayBuffer
import org.apache.commons.compress.archivers.zip.ZipFile
import com.betterdocs.crawler.ZipBasicParser
import scala.util.Try
import com.betterdocs.indexer.JavaMethodTransactionGenerator
import scala.collection.JavaConversions.mapAsScalaMap
import org.elasticsearch.spark._

object IndexRepoJob extends SparkJob with NamedRddSupport {

  //val githubPath = "/home/sachint/github/"

  def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid
  }

  def runJob(sc: SparkContext, config: Config): Any = {
    val transactionSize = Try(Option(config.getString("size")).getOrElse("5").toInt).getOrElse(5)
    val githubPath = Option(config.getString("repos.path")).getOrElse("/home/sachint/github/")

    //Read repos from locally stored RDDs if available else from zips **/
    val repos = readOrSaveRepos(sc, githubPath)
    val repoDetails = repos.map { f =>
      val (files, score, orgsName) = f
      val trns = new JavaMethodTransactionGenerator().generateTransactions(files.toMap, List(), score.getOrElse(0), orgsName.getOrElse("ErrorRecord"))
      //(trns._1, trns._2, trns._3, trns._4, score.getOrElse(0), orgsName.getOrElse("N/A"))
      println("Parsed: "+ trns._1.fullRepoName)
      trns
    }

    repoDetails.map(f => f._1).map { x => Map("repoName" -> x.fullRepoName, "orgsName" -> x.orgsName, "score" -> x.score, "declaredPckgs" -> x.pckgDeclCountMap.map(f=> Map("pckg"->f._1, "count"->f._2)), "usedPckgs" -> x.pckgUsedCountMap.map(f=> Map("pckg"->f._1, "count"->f._2))) }.saveToEs("parsed/repos")
    repoDetails.flatMap(f => f._2).filter { f => {/*println(f.callStack.size);*/f.callStack.size > transactionSize} }.map { x => Map("method" -> x.methodUrl, "class" -> x.className, "events" -> x.callStack, "istest" -> x.isTest, "pckg" -> x.pckg, "usedPckgs" -> x.usedPckgs) }.saveToEs("parsed/transactions")
    //val transactions = repoDetails.flatMap { x => x._4 }.filter { x => x._2.size() > transactionSize }
    //transactions.map(f=> Map("method"->f._1, "events"-> f._2)).saveToEs("parsed/transactions")
    "Done"
  }

  def readOrSaveRepos(sc: SparkContext, githubPath: String): RDD[(ArrayBuffer[(String, String)], Option[Int], Option[String])] = {
    val repos = sc.binaryFiles(githubPath).map { x =>
      val zipFileName = x._1.stripPrefix("file:")
      println("Reading Zip:  " + zipFileName)
      val z = new ZipFile(zipFileName)
      val score = getGitScore(zipFileName)
      val orgsName = getOrgsName(zipFileName)
      val repoName = getRepoName(zipFileName)
      // Ignoring exclude packages.
      (ZipBasicParser.readFilesAndPackages(z)._1, score, orgsName)
    }
    repos
  }

  /**
   * This currently uses star counts for a repo as a score.
   */
  def getGitScore(f: String): Option[Int] = {
    Try(f.stripSuffix(".zip").split("~").last.toInt).toOption
  }

  def getOrgsName(f: String): Option[String] = {
    Try(f.stripSuffix(".zip").split("~").tail.head).toOption
  }

  def getRepoName(f: String): Option[String] = {
    Try(f.stripSuffix(".zip").split("~").tail.tail.head).toOption
  }

}
