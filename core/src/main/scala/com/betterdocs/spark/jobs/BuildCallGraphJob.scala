package com.betterdocs.spark.jobs

import spark.jobserver.SparkJob
import spark.jobserver.NamedRddSupport
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import spark.jobserver.SparkJobValidation
import spark.jobserver.SparkJobValid
import org.elasticsearch.spark._
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import scala.collection.mutable.Buffer
import scala.collection.mutable.LinkedHashMap
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

class BuildCallGraphJob extends SparkJob with NamedRddSupport {

  
  def runJob(sc: SparkContext, config: Config): Any = {
    val support = Option(config.getString("support")).getOrElse("0.01").toDouble //1%
    val qryString = constructQueryString(config)
    println(qryString)
    val transactions = sc.esRDD("parsed/transactions", Map("es.query" -> qryString, "es.read.metadata"->"true", "es.read.metadata.field"->"meta")).
      map(f => {
        //println("Score: "+ f._2.get("meta").get.asInstanceOf[LinkedHashMap[String,String]].get("_score").get.asInstanceOf[Double]);
        (f._2.get("method").getOrElse(""), f._2.get("events").get.asInstanceOf[Buffer[String]])}).cache()
    //.map(f=> {(f._1, f._2.get(f._1).toList)})
    
    val count = transactions.count()
    
    //println(transactions.first())

    println("Count: "+ count + " , ratio: " + (count*support))
    import collection.JavaConversions._
    import org.apache.spark.SparkContext._
    //val tuplesRDD = transactions.map(f => f._2 zip f._2.tail).map(f=> f.map(x => (x._1, x._2, 1)))
    //val filter = Set("java.io.File", "FileOutputStream")

    //val filteredTrans = transactions.filter(f => f._2.exists { x => filter.forall { y => { x.toString.contains(y) } } })
    val filteredTrans = transactions
    val tuplesRDD = filteredTrans.map(f => f._2 zip f._2.tail).map(f => f.map(x => ((x._1, x._2), 1))).flatMap(f => f)

    val vertices = filteredTrans.flatMap(f => f._2).map(f=>(f,1)).reduceByKey(_+_).filter(f=>f._2>count*support).map(f=>f._1).zipWithUniqueId()
    val map = vertices.collectAsMap

    val edges = tuplesRDD.reduceByKey(_ + _)//.filter(f => (!f._1._1.equals(f._1._2) && f._2 > count*support))
      .map(f => Edge(map.get(f._1._1).getOrElse(0), map.get(f._1._2).getOrElse(0), f._2))
    
    val v = vertices.map { x => Map("id"-> x._2.toString(), "label"->x._1, "size"->5.toString(), "color"->"rgb(1,179,255)") }.collect
    val e = edges.zipWithUniqueId().map(f=> Map("id"->f._2.toString(), "source"->f._1.srcId.toString(), "target"->f._1.dstId.toString(), "type"->"arrow")).collect
    
    //val callGraph = Graph(vertices.map(f => (f._2, (f._1, 0))), edges, ("Null", 0))

    //val triplets = callGraph.triplets.map(f => ((f.srcId, f.srcAttr._1), (f.dstId, f.dstAttr._1), f.attr))

    //triplets.collect()
    implicit val formats = Serialization.formats(NoTypeHints)
    write(Map("nodes"->v, "edges"->e)) 

  }

  def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid
  }
  
  
  def constructQueryString(config: Config): String = {
    val excludeTests = Option(config.getString("excludeTests")).getOrElse("false").toBoolean
    var queryPrefix = """{"query": {   "bool" : {    "should" : ["""
    if(excludeTests){
      queryPrefix = """{"query": {   "bool" : {  "must" : {"term" : {"istest": false},  "should" : ["""  
    }
    
    val queryPostfix = """  ],    "minimum_should_match" : 1,    "boost" : 1.0      }  }}""" 
    val filter = config.getString("class").split(",").map { x => x.trim() }.toSet.mkString("""{"term" : { "events" : """", """" }},{"term" : { "events" : """", """"}}""")
    queryPrefix + filter + queryPostfix;    
  }
}
