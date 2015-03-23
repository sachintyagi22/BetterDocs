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

import sbt._
import sbt.Keys._

object BetterDocsBuild extends Build {

  lazy val root = Project(
    id = "betterdocs",
    base = file("."),
    settings = betterDocsSettings,
    aggregate = Seq(core)
  )

  lazy val core = Project("core", file("core"), settings = coreSettings)

  val scalacOptionsList = Seq("-encoding", "UTF-8", "-unchecked", "-optimize", "-deprecation",
    "-feature")

  def coreSettings = betterDocsSettings ++ Seq(libraryDependencies ++= Dependencies.betterDocs)

  def betterDocsSettings =
    Defaults.coreDefaultSettings ++ Seq (
      name := "BetterDocs",
      organization := "com.betterdocs",
      version := "0.0.1-SNAPSHOT",
      scalaVersion := "2.11.6",
      scalacOptions := scalacOptionsList,
     // retrieveManaged := true, // enable this if we need jars of dependencies.
      crossPaths := false,
      fork := true,
      javaOptions += "-Xmx2048m" // For running spark job.
    )

}

object Dependencies {

  val spark = "org.apache.spark" %% "spark-core" % "1.2.1"
  val parserCombinator = "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.3"
  val scalaTest = "org.scalatest" %% "scalatest" % "2.2.4" % "test"
  val sl4j = "org.slf4j" % "slf4j-log4j12" % "1.7.10"
  val javaparser = "com.github.javaparser" % "javaparser-core" % "2.0.0"
  val mllib = "org.apache.spark"  % "spark-mllib_2.11" % "1.2.1"
  val betterDocs = Seq(spark, parserCombinator, scalaTest, sl4j, javaparser, mllib)

  // transitively uses
  // commons-httpclient-3.1
  // commons-io-2.4
  // json4s-jackson_2.11-3.2.10
  // json4s-ast_2.11-3.2.10.jar
  // commons-compress-1.4.1
  // "com.typesafe" % "config" % "1.2.1"

}
