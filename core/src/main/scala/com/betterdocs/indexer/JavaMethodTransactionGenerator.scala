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
import scala.collection.mutable
import collection.JavaConversions._
import com.betterdocs.crawler.Repository
import com.betterdocs.indexer.JavaFileIndexerHelper.fileNameToURL

case class RepoDetails(fullRepoName: String, score: Int, orgsName: String, pckgDeclCountMap: Map[String, Int], pckgUsedCountMap: Map[String, Int])
case class ClassMethodDetails(methodUrl: String, callStack: List[String], className: String, usedPckgs: Set[String], pckg: String, isTest: Boolean)

trait TransactionGenerator extends Serializable {
 
  def generateTransactions(files: Map[String, String], excludePackages: List[String], score: Int,
      orgsName: String, repo: Option[Repository]): (RepoDetails, mutable.Set[ClassMethodDetails])

}

class JavaMethodTransactionGenerator extends TransactionGenerator {
  
  override def generateTransactions(files: Map[String, String], excludePackages: List[String],
      score: Int, orgsName: String, repo: Option[Repository]): (RepoDetails, mutable.Set[ClassMethodDetails]) = {

    //var methodCallMap = new java.util.HashMap[String, java.util.List[String]]()
    var methodList = mutable.Set[ClassMethodDetails]();
    /*var repoDeclaredPackages = new java.util.HashSet[String]()
    var repoUsedPackages = new java.util.HashSet[String]()*/
    
    var repoUsedPackgesList = List[String]()
    var repoDeclaredPackgesList = List[String]()
    var fullRepoName = ""
    
    for (file <- files) {
      val (fileName, fileContent) = file
      /**val (repoName, actualFileName) = fileName.splitAt(fileName.indexOf('/'))
      val (actualRepoName, branchName) = repoName.splitAt(fileName.indexOf('-'))
      val fullGithubURL = s"""http://github.com/$orgsName/$actualRepoName/blob/${
        branchName
          .stripPrefix("-")
      }$actualFileName""" **/
      
        
      val fullGithubURL  = fileNameToURL(repo.getOrElse(Repository.invalid), fileName)
      
      val parser = new JavaFileParser()
      parser.parse(fileContent, fullGithubURL, score) 
      
      val thisMap = parser.getMethodCallStack;
      for(entry <- thisMap){
        methodList.add(ClassMethodDetails(entry._1, entry._2.toList, parser.getClazzName , parser.gedUsedPackages.toSet, parser.getDeclaredPackage, parser.isTestClass))        
      }
      
      //methodCallMap.putAll(thisMap)
      
      //fullRepoName = s"$orgsName/$actualRepoName"
      val name = repo.getOrElse(Repository.invalid).name
      fullRepoName = s"$orgsName/$name"
      
      repoUsedPackgesList = List.concat(repoUsedPackgesList, parser.gedUsedPackages.toArray(Array("")).toSeq).filter(p=>p!=null)
      repoDeclaredPackgesList = List.concat(repoDeclaredPackgesList, List(parser.getDeclaredPackage)).filter(p=>p!=null)
      
    }
    
    val pckgUsedCountMap = repoUsedPackgesList.map { x => (x, 1L) }.groupBy(f=> f._1).mapValues(f=>f.size).map(identity)
    val pckgDeclCountMap = repoDeclaredPackgesList.map { x => (x, 1L) }.groupBy(f=> f._1).mapValues(f=>f.size).map(identity)
    
    
    (RepoDetails(fullRepoName, score, orgsName, pckgDeclCountMap, pckgUsedCountMap), methodList)
  }

}


