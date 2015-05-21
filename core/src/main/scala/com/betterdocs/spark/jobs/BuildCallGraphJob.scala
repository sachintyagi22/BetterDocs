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
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.graphx.TripletFields
import org.apache.spark.graphx.EdgeContext

class BuildCallGraphJob extends SparkJob with NamedRddSupport {

  
  def runJob(sc: SparkContext, config: Config): Any = {
    val support = Option(config.getString("support")).getOrElse("0.25").toDouble //1%
    val classSet = config.getString("class").split(",").map { x => x.trim() }.toSet;
    val excludeTests = Option(config.getString("excludeTests")).getOrElse("false").toBoolean
    val qryString = constructQueryString(config)
    println(qryString)
    val transactions = sc.esRDD("parsed/transactions", Map("es.query" -> qryString, "es.read.metadata"->"true", "es.read.metadata.field"->"meta")).
    filter(x=>(!excludeTests || x._2.get("istest").getOrElse("false").equals(false) )).
      map(f => {(f._2.get("method").getOrElse(""), f._2.get("events").get.asInstanceOf[Buffer[String]])}).cache()
    
    println("counting..." + transactions)
    val count = transactions.count()

    println("Count: "+ count + " , ratio: " + (count*support))
    import collection.JavaConversions._
    import org.apache.spark.SparkContext._
    val filteredTrans = transactions
    val tuplesRDD = filteredTrans.map(f => f._2 zip f._2.tail).map(f => f.map(x => ((x._1, x._2), 1))).flatMap(f => f)

    val threshold = count*support;
    
    val vertices = filteredTrans.flatMap(f => f._2).map(f=>(f,1)).reduceByKey(_+_).map(f=>f._1).zipWithUniqueId()
    val map = vertices.collectAsMap

    val edges = tuplesRDD.reduceByKey(_ + _).map(f => Edge(map.get(f._1._1).getOrElse(0), map.get(f._1._2).getOrElse(0), f._2))
    
    val callGraph = Graph(vertices.map(f => (f._2, (f._1, 0))), edges, ("Null", 0))

    val significantVertices = callGraph.aggregateMessages[(String, Int)](ctx => {
      ctx.sendToDst(ctx.dstAttr._1 ,ctx.attr)
      ctx.sendToSrc(ctx.srcAttr._1 ,ctx.attr)
    }, (a, b) => (a._1, a._2+b._2)).filter(f=> f._2._2>(threshold))
    
    val vids = significantVertices.map(f=>f._1).collect().toSet
    val significantEdges = edges.filter(x=> vids.contains(x.srcId) && vids.contains(x.dstId))
 
    //val significantCallGraph = Graph(significantVertices, edges, ("Null", 0))
    
    val v = significantVertices.map { x => Map("id"-> x._1.toString(), "label"->x._2._1, "size"->x._2._2, "color"->"rgb(1,179,255)") }.collect
    val e = significantEdges.zipWithUniqueId().map(f=> Map("id"->f._2.toString(), "source"->f._1.srcId.toString(), "target"->f._1.dstId.toString(), "type"->"arrow", "size"->f._1.attr)).collect
    
    implicit val formats = Serialization.formats(NoTypeHints)
    write(Map("nodes"->v, "edges"->e)) 

  }

  def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid
  }
  
  
  def constructQueryString(config: Config): String = {
    
    var queryPrefix = """{"query": {   "bool" : {    "should" : ["""
    /*if(excludeTests){
      queryPrefix = """{"query": {   "bool" : {  "must" : {"term" : {"istest": false},  "should" : ["""  
    }*/
    
    val queryPostfix = """  ],    "minimum_should_match" : 1,    "boost" : 1.0      }  }}""" 
    val filter = config.getString("class").split(",").map { x => x.trim() }.toSet.mkString("""{"term" : { "events" : """", """" }},{"term" : { "events" : """", """"}}""")
    queryPrefix + filter + queryPostfix;    
  }
}
