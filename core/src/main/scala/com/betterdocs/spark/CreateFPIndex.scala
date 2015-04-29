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

import scala.collection.mutable.ArrayBuffer
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import org.apache.commons.compress.archivers.zip.ZipFile
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import com.betterdocs.configuration.BetterDocsConfig
import com.betterdocs.crawler.ZipBasicParser
import com.betterdocs.crawler.ZipBasicParser.listAllFiles
import com.betterdocs.crawler.ZipBasicParser.readFilesAndPackages
import com.betterdocs.indexer.JavaMethodTransactionGenerator
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions.mapAsScalaMap
import org.apache.spark.mllib.fpm.{ FPGrowth, FPGrowthModel }
import scala.reflect.ClassTag
import scala.collection.mutable.ListBuffer
import java.nio.file.Files
import java.nio.file.Paths
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import java.util.regex.Pattern
import java.util.regex.Matcher
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Edge



object CreateFPIndex {
  
  val baseDir = "output-fp"
  val transactionFilePath = s"/$baseDir/transactions/"
  val reposLocation = s"/$baseDir/repos/"
  val reposDetailsPath = s"/$baseDir/repodetails/objects"
  val verticalDataPath = s"/$baseDir/vertical/data"
  val verticalDataTextPath = s"/$baseDir/vertical/text/"
  val patternsDataTextPath = s"/$baseDir/patterns/"
  val filesDataTextPath = s"/$baseDir/files/"
  val githubPath = "/home/sachint/github/"
  val repoDeclaredPath = s"/$baseDir/repodetails/declared/"
  val repoUsedPath = s"/$baseDir/repodetails/used/"
  val repoDependencyPath = s"/$baseDir/dependency/"
  val graphPath = s"/$baseDir/graph/"

  def main(args: Array[String]): Unit = {

    val minSupport = 25;
    val transactionSize = 4;

    val conf = new SparkConf()
      .setMaster(BetterDocsConfig.sparkMaster)
      .setAppName("CreateFPIndexJob")
      .set("spark.files.overwrite", "true")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("spark.driver.memory", "3g")

    val sc = new SparkContext(conf)
    

    //Read repos from locally stored RDDs if available else from zips **/
    val repos = readOrSaveRepos(sc)
    // Output the java files from repos (plus other info) in the format that can be bulk uploaded to ES.
    if (args.contains("file")) { saveFilesForES(repos) }

    val repoDetails = readOrSaveRepoDetails(sc, repos)

    val transactions = repoDetails.flatMap { x => x._4 }.filter { x => x._2.size() > transactionSize }
    
    import collection.JavaConversions._
    import org.apache.spark.SparkContext._
    //val tuplesRDD = transactions.map(f => f._2 zip f._2.tail).map(f=> f.map(x => (x._1, x._2, 1)))
    
    
    val tuplesRDD = transactions.map(f => f._2 zip f._2.tail).map(f=> f.map(x => ((x._1, x._2), 1))).flatMap(f=>f)

    val vertices = transactions.flatMap(f=>f._2).distinct().zipWithUniqueId()
    val map = vertices.collectAsMap
    
    val edges = tuplesRDD.reduceByKey(_+_).map(f=> Edge(map.get(f._1._1).getOrElse(0), map.get(f._1._2).getOrElse(0), f._2))
    val callGraph = Graph(vertices.map(f=>(f._2, (f._1,0))), edges, ("Null",0))
    callGraph.vertices.saveAsTextFile(graphPath)
    
    val filter = "java.util.concurrent.Executor"
    
    val subgraph = callGraph.ops.pregel(0)(vprog = (id, attr, msg) => {
      //println(">>>>>>> Recieved msg " + msg + " at " + vertName._1)
      (attr._1, msg)
    }, sendMsg = (triplet) => {
      if((triplet.srcAttr._1.contains(filter) && triplet.srcAttr._2 == 0) || (triplet.srcAttr._2 > triplet.dstAttr._2)){
        println(">>>>>>>>>>>>>>>>> Sending message from " + triplet.srcAttr._1 + " to " + triplet.dstAttr._1)
        Iterator((triplet.dstId, 1))
      }else{
        Iterator.empty
      }
      Iterator.empty
    }, mergeMsg = (a,b)=> {
      a+b
    })

    
    println(">>>>>>>>>> \n\n\n" + subgraph.triplets.collect.mkString("\n"))
    
    
    /*callGraph.subgraph(epred = (eTriplet) => {
      true
    }, vpred = (id, attr)=>  {
      true
    })*/

    //Create/load/store vertical views of transactions. Needed for getting file info for patterns.
    val verticalDataMap = readOrSaveVerticalData(sc, transactions, minSupport)

    //Run FPGrowth and get frequent patterns.
    //Also use vertical data view to find transaction info for the patterns.
    if (args.contains("fpgrowth")) {
      val data = transactions.map(f => f._2.toArray().distinct)

      val fpg = new FPGrowth();
      fpg.setMinSupport(minSupport)
      val model = fpg.run(data)
      val fs = model.freqItemsets
      //val fi = model.freqItems

      //val verticalMap = verticalDataMap.filter(f => fi.exists { x => x.equals(f._1) }).collect().toMap;
      val verticalMap = verticalDataMap.collect().toMap;

      if (args.contains("pattern")) {
        fs.map { x =>

         /* val sets = x.items.map { x => verticalMap.get(x) }.map { x => x.getOrElse(Set.empty) }
          val result = getIntersectionOfSets(sets)
          var ss = result.toSeq.sortWith((a, b) => {
            Try(a.split("#").last.toInt).getOrElse(0) < Try(b.split("#").last.toInt).getOrElse(0)
          })
          if (ss.size > 5) {
            ss = ss.take(5);
          }*/
          val ss = Seq.empty[String];
          toJson(x.items, x.freq, ss, true)
        }.repartition(250).saveAsTextFile(patternsDataTextPath);

      }

    }

  }

  def saveFilesForES(repos: org.apache.spark.rdd.RDD[(scala.collection.mutable.ArrayBuffer[(String, String)], Option[Int], Option[String])]) = {
    {
      val javafileData = repos.flatMap { f =>
        val (files, score, orgsName) = f
        val orgsNameStr = orgsName.get
        val list = new ListBuffer[Tuple9[String, String, String, String, String, String, String, String, String]]();
        for (file <- files.toMap) {
          val (fileName, fileContent) = file
          val (repoName, actualFileName) = fileName.splitAt(fileName.indexOf('/'))
          val (actualRepoName, branchName) = repoName.splitAt(fileName.indexOf('-'))
          val fullGithubURL = s"""http://github.com/$orgsNameStr/$actualRepoName/blob/${
            branchName
              .stripPrefix("-")
          }$actualFileName"""

          list.append((fileName, repoName, actualFileName, actualRepoName, branchName, fullGithubURL, score.get.toString(), orgsName.get, fileContent))
        }
        list
      }.map { x => javaFileToJSON(x) }.saveAsTextFile(filesDataTextPath);
    }
  }

  def getIntersectionOfSets(sets: Array[Set[String]]): scala.collection.mutable.Set[String] = {
    var tIntersection = new scala.collection.mutable.HashSet[String]()
    for (s <- sets) {

      if (tIntersection.isEmpty) {
        tIntersection = tIntersection ++ s
      } else {
        val temp = s.intersect(tIntersection)
        tIntersection.clear()
        tIntersection = tIntersection ++ temp
      }

    }

    tIntersection
  }

  def readOrSaveRepos(sc: SparkContext): RDD[(ArrayBuffer[(String, String)], Option[Int], Option[String])] = {
    //Read repos from locally stored RDDs if available else from zips **/
    var repos: RDD[(ArrayBuffer[(String, String)], Option[Int], Option[String])] = null

    if (Files.exists(Paths.get(reposLocation))) {
      repos = sc.objectFile[(ArrayBuffer[(String, String)], Option[Int], Option[String])](reposLocation)
    }

    //val repoScoreMap = collection.mutable.HashMap[String, Int]()
    if (repos == null) {
      repos = sc.binaryFiles(githubPath).map { x =>
        val zipFileName = x._1.stripPrefix("file:")
        println("Reading Zip:  " + zipFileName)
        val z = new ZipFile(zipFileName)
        val score = getGitScore(zipFileName)
        val orgsName = getOrgsName(zipFileName)
        val repoName = getRepoName(zipFileName)
        //repoScoreMap.put(orgsName+"/"+repoName, score.getOrElse(0))
        // Ignoring exclude packages.
        (ZipBasicParser.readFilesAndPackages(z)._1, score, orgsName)
      }
      //repos.saveAsObjectFile(reposLocation);
    }

    repos
  }



  def readOrSaveRepoDetails(sc: SparkContext, repos: RDD[(ArrayBuffer[(String, String)], Option[Int], Option[String])])= {
    //var repoDetails: RDD[(String, Map[String,Int], Map[String,Int], java.util.HashMap[String, java.util.List[String]])] = null

    /*if (Files.exists(Paths.get(reposDetailsPath))) {
      repoDetails = sc.objectFile[(String, Map[String,Int], Map[String,Int], java.util.HashMap[String, java.util.List[String]])](reposDetailsPath)
    }*/

    /*if (repoDetails == null || repoDetails.count() < 1) {
      repoDetails = repos.map { f =>
        val (files, score, orgsName) = f
        val trns = new JavaMethodTransactionGenerator().generateTransactions(files.toMap, List(), score.getOrElse(0), orgsName.getOrElse("ErrorRecord"))
        (trns._1, trns._2, trns._3, trns._4, score.getOrElse(0), score.getOrElse(0))
      }
      //repoDetails.saveAsObjectFile(reposDetailsPath)
    }*/

    val repoDetails = repos.map { f =>
        val (files, score, orgsName) = f
        val trns = new JavaMethodTransactionGenerator().generateTransactions(files.toMap, List(), score.getOrElse(0), orgsName.getOrElse("ErrorRecord"))
        (trns._1, trns._2, trns._3, trns._4, score.getOrElse(0), orgsName.getOrElse("N/A"))
     }
    
    def repoPkgToString(repo: String, decpkgs: Map[String, Int], usedpkgs: Map[String, Int], score: Int, orgsName: String): String = {
      implicit val formats = Serialization.formats(NoTypeHints)
      """|{ "index" : { "_index" : "fpgrowth", "_type" : "repodetails" } }
         |""".stripMargin + write(Map("repo" -> repo, 
             "declaredPackages" -> write(decpkgs.toSeq), 
             "usedPackages" -> write(usedpkgs.toSeq),
              "score" -> score,
              "orgsName" -> orgsName
             ))
    }
    
    /*val dependency = repoDetails.map(f => {
      (f._1, f._3.map(d =>{
        repoDetails.filter(f=> f._2.contains(d._1)).map(f=> f._1).collect().headOption.getOrElse("")
      }).toSet)
    }).saveAsTextFile(repoDependencyPath)*/
    
    //Declared packages used enough elsewhere
    //val declaredPackages = repoDetails.map(f => (f._1, f._2.toArray().toSet.filter { x => fi.exists { y => y.toString().equals(x) }})).map(f => repoPkgToString(f._1, f._2))
    val packageDetails = repoDetails.map(f => repoPkgToString(f._1, f._2, f._3, f._5, f._6))

    packageDetails.saveAsTextFile(repoDeclaredPath)
    

    repoDetails

  }


  def readOrSaveVerticalData(sc: SparkContext, transactions: RDD[(String, java.util.List[String])], minSupport: Int): RDD[(Object, Set[String])] = {
    var verticalDataMap: RDD[(Object, Set[String])] = null;
    if (Files.exists(Paths.get(verticalDataPath))) {
      verticalDataMap = sc.objectFile[(Object, Set[String])](verticalDataPath)
    }

    if (verticalDataMap == null || verticalDataMap.count() < 1) {
      import scala.collection.JavaConverters._
      import org.apache.spark.SparkContext._
      val verticalData = transactions.flatMap(f => f._2.toArray().distinct.map { x => (x, f._1) })
      verticalDataMap = verticalData.reduceByKey(_ + "^" + _).map(f => (f._1, f._2.split("\\^").toSet))
      verticalDataMap = verticalDataMap.filter(f => { f._2.size >= minSupport })
      verticalDataMap.saveAsObjectFile(verticalDataPath)
      verticalDataMap.map(f => toVerticalJson(f._1.toString(), f._2, true)).saveAsTextFile(verticalDataTextPath)
    }

    verticalDataMap

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

  def toJson[Item: ClassTag](items: Array[Item], freq: Long, set: Seq[String], addESHeader: Boolean = false): String = {
    implicit val formats = Serialization.formats(NoTypeHints)
    if (addESHeader) {
      """|{ "index" : { "_index" : "fpgrowth", "_type" : "patterns" } }
         |""".stripMargin + write(Map("freq" -> freq, "length" -> items.length, "body" -> items, "locations" -> write(set)))
    } else { "[ " + freq + " ] : " + write(items) }
  }

  def toVerticalJson(key: String, trans: Set[String], addESHeader: Boolean = false): String = {
    implicit val formats = Serialization.formats(NoTypeHints)
    val p = Pattern.compile("([a-zA-Z]+.*)\\.([a-zA-Z]*)", Pattern.CASE_INSENSITIVE);
    def getMatches(p:Pattern, s:String, acc: List[String]):List[String] ={
      val matcher = p.matcher(s);
      if (matcher.find()) {
        val group = matcher.group(1);
        getMatches(p, group, acc.+:(group));
      }else{
        acc
      }
    }
    val index = getMatches(p, key, List());
    
    if (addESHeader) {
      """|{ "index" : { "_index" : "fpgrowth", "_type" : "vertical" } }
         |""".stripMargin + write(Map("event" -> key, "event_index" -> write(index), "transactions" -> write(trans), "freq"-> trans.size))
    } else { "[ " + key + " ] : " + write(trans) }
  }

  def javaFileToJSON(t: Tuple9[String, String, String, String, String, String, String, String, String]): String = {
    implicit val formats = Serialization.formats(NoTypeHints)

    //(fileName, repoName, actualFileName, actualRepoName, branchName, fullGithubURL, score.toString(), orgsName.toString())
    """|{ "index" : { "_index" : "betterdocs", "_type" : "javafile" } }
       |""".stripMargin + write(Map("name" -> t._1, "repoName" -> t._2, "actualFileName" -> t._3, "actualRepoName" -> t._4,
      "branchName" -> t._5, "fullGithubURL" -> t._6, "score" -> t._7, "orgsName" -> t._8, "body" -> t._9))
  }
}


object Test{
  def main(args: Array[String]): Unit = {
    println(("b", "a").equals(("a", "b")))
  }
}