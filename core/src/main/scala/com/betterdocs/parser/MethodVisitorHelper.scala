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

package com.betterdocs.parser

import java.util.{ArrayList, HashMap}

import com.betterdocs.crawler.Repository


object MethodVisitorHelper {

  import com.betterdocs.indexer.JavaFileIndexerHelper._

  def getImportsWithMethodAndLineNumbers(parser: MethodVisitor):
  Map[String, Map[String, List[Int]]] = {
    val imports = parser.getImportDeclMap
    val importWithMethodAndLineNumbers =
      javatoScalaMap(parser.getImportsWithMethodAndLineNumber)
    importWithMethodAndLineNumbers.filter {
      case (importName, methodAndLineNumbers) => imports.containsValue(importName)
    }
  }

  def javatoScalaMap(javaHashMap: HashMap[String, HashMap[String, ArrayList[Integer]]]):
  Map[String, Map[String, List[Int]]] = {
    import scala.collection.JavaConversions._
    (javaHashMap map {
      case (k, v) => k -> v.toMap.map { case (k, v) => k -> v.toList.distinct.map(_.toInt)}
    }).toMap

  }

  def extractMethodsAndLineNumbers(files: Map[String, String],
                                   repo: Option[Repository]): Iterable[MethodIndexEntry] = {
    val m = new MethodVisitor()
    val r = repo.getOrElse(Repository.invalid)
    for (file <- files)
    yield {
      val (fileName, fileContent) = file
      m.parse(fileContent, fileName)
      val r = repo.getOrElse(Repository.invalid)
      val fullGithubURL = fileNameToURL(r, fileName)
      val m1 = getImportsWithMethodAndLineNumbers(m)
      val methodTokens = m1.map {
        case (importName, methodAndLineNumbers) =>
          MethodToken(importName, methodAndLineNumbers.map {
            case (k, v) => MethodAndLines(k, v)
          }.toList)
      }
      MethodIndexEntry(r.id, fullGithubURL, methodTokens.toSet)
    }
  }
}

case class MethodIndexEntry(repoId: Int, fileName: String,
                            methodTokens: Set[MethodToken])

case class MethodToken(importName: String, methodAndLineNumbers: List[MethodAndLines])

case class MethodAndLines(methodName: String, lineNumbers: List[Int])

