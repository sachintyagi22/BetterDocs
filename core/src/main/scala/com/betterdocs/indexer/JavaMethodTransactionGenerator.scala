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

package com.betterdocs.indexer

import java.util.regex.Pattern
import org.apache.commons.lang3.StringUtils
import scala.util.Try
import scala.collection.mutable.LinkedHashSet.Entry
import scala.collection.mutable.HashMap
import com.betterdocs.parser.JavaFileParser
import java.util.HashSet
import scala.collection.mutable.ListBuffer

trait TransactionGenerator extends Serializable {
 
  def generateTransactions(files: Map[String, String], excludePackages: List[String], score: Int,
      orgsName: String): (String, Map[String,Int], Map[String,Int], java.util.HashMap[String, java.util.List[String]])

}

class JavaMethodTransactionGenerator extends TransactionGenerator {

  override def generateTransactions(files: Map[String, String], excludePackages: List[String],
      score: Int, orgsName: String): (String, Map[String,Int], Map[String,Int], java.util.HashMap[String, java.util.List[String]]) = {

    var methodCallMap = new java.util.HashMap[String, java.util.List[String]]()
    /*var repoDeclaredPackages = new java.util.HashSet[String]()
    var repoUsedPackages = new java.util.HashSet[String]()*/
    
    var repoUsedPackgesList = List[String]()
    var repoDeclaredPackgesList = List[String]()
    var fullRepoName = ""
    
    for (file <- files) {
      val (fileName, fileContent) = file
      val (repoName, actualFileName) = fileName.splitAt(fileName.indexOf('/'))
      val (actualRepoName, branchName) = repoName.splitAt(fileName.indexOf('-'))
      val fullGithubURL = s"""http://github.com/$orgsName/$actualRepoName/blob/${
        branchName
          .stripPrefix("-")
      }$actualFileName"""
      
      val parser = new JavaFileParser()
      parser.parse(fileContent, fullGithubURL, score) 
      
      val thisMap = parser.getMethodCallStack;
      
      methodCallMap.putAll(thisMap)
      
      fullRepoName = s"$orgsName/$actualRepoName"
      
      repoUsedPackgesList = List.concat(repoUsedPackgesList, parser.gedUsedPackages.toArray(Array("")).toSeq)
      repoDeclaredPackgesList = List.concat(repoDeclaredPackgesList, List(parser.getDeclaredPackage))
      
    }
    
    val pckgUsedCountMap = repoUsedPackgesList.map { x => (x, 1L) }.groupBy(f=> f._1).mapValues(f=>f.size).map(identity)
    val pckgDeclCountMap = repoDeclaredPackgesList.map { x => (x, 1L) }.groupBy(f=> f._1).mapValues(f=>f.size).map(identity)
    
    (fullRepoName, pckgDeclCountMap, pckgUsedCountMap, methodCallMap)
  }

}


