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
import com.betterdocs.crawler.{Repository, ZipBasicParser}
import com.betterdocs.indexer.JavaASTBasedIndexer
import com.betterdocs.parser.RepoFileNameParser
import org.apache.commons.compress.archivers.zip.ZipFile
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

object CreateIndexJob {

  case class SourceFile(repoId: Int, fileName: String, fileContent: String)

  def mapToSourceFiles(repo: Option[Repository], map: ArrayBuffer[(String, String)]) = {
    val repo2 = repo.getOrElse(Repository.invalid)
    import com.betterdocs.indexer.JavaFileIndexerHelper._

    map.map(x => SourceFile(repo2.id, fileNameToURL(repo2, x._1), x._2)).toSet
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster(BetterDocsConfig.sparkMaster)
      .setAppName("CreateIndexJob")
    val sc = new SparkContext(conf)
    val zipFileNameRDD = sc.binaryFiles(BetterDocsConfig.githubDir).map { case (zipFile, _) =>
      zipFile.stripPrefix("file:")
    }
    val zipFileExtractedRDD = zipFileNameRDD.flatMap { zipFileName =>
      // Ignoring exclude packages.
      val zipFileOpt = Try(new ZipFile(zipFileName)).toOption
      zipFileOpt.map { zipFile =>
        val (filesMap, packages) = ZipBasicParser.readFilesAndPackages(zipFile)
        (filesMap, RepoFileNameParser(zipFileName), packages)
      }
    }

    // Create indexes for elastic search.
    zipFileExtractedRDD.map { f =>
      val (files, repo, packages) = f
      (repo, new JavaASTBasedIndexer()
        .generateTokens(files.toMap, packages, repo), mapToSourceFiles(repo, files))
    }.flatMap { case (Some(a), b, c) =>
      Seq(toJson(a, isToken = false), toJson(b, isToken = true), toJson(c, isToken = false))
    case _ => Seq()
    }.saveAsTextFile(BetterDocsConfig.sparkIndexOutput)

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

  def toJson[T <:AnyRef <% Product with Serializable](t: Set[T], isToken: Boolean): String = {
    (for (item <- t) yield toJson(item, addESHeader = true, isToken = isToken)).mkString("\n")
  }

  def toJson[T <: AnyRef <% Product with Serializable](t: T, addESHeader: Boolean = true,
      isToken: Boolean = false ): String = {
    import org.json4s._
    import org.json4s.jackson.Serialization
    import org.json4s.jackson.Serialization.write
    implicit val formats = Serialization.formats(NoTypeHints)
    val indexName = t.productPrefix.toLowerCase
    if (addESHeader && isToken) {
      """|{ "index" : { "_index" : "betterdocs", "_type" : "custom" } }
         | """.stripMargin + write(t)
    } else if (addESHeader) {
      s"""|{ "index" : { "_index" : "$indexName", "_type" : "type$indexName" } }
          |""".stripMargin + write(t)
    } else "" + write(t)

  }
}
