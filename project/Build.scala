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
import de.johoop.findbugs4sbt.FindBugs._
import de.johoop.cpd4sbt.CopyPasteDetector._
import de.johoop.cpd4sbt.Language

object BetterDocsBuild extends Build {

  lazy val root = Project(
    id = "betterdocs",
    base = file("."),
    settings = betterDocsSettings,
    aggregate = aggregatedProjects
  )

  lazy val core = Project("core", file("core"), settings = coreSettings)

  lazy val ideaPlugin = Project("ideaPlugin", file("plugins/idea/betterdocsidea"), settings =
    pluginSettings)

  val scalacOptionsList = Seq("-encoding", "UTF-8", "-unchecked", "-optimize", "-deprecation",
    "-feature")

  // This is required for plugin devlopment.
  val ideaLib = sys.env.get("IDEA_LIB").orElse(sys.props.get("idea.lib"))

  def aggregatedProjects: Seq[ProjectReference] = {
    if (ideaLib.isDefined) {
      Seq(core, ideaPlugin)
    } else {
      println("""[warn] Plugin project disabled. To enable append -Didea.lib="idea/lib" to JVM params in SBT settings or while invoking sbt (incase it is called from commandline.). """)
      Seq(core)
    }
  }

  def pluginSettings = betterDocsSettings ++ (if (!ideaLib.isDefined) Seq() else 
    findbugsSettings ++ codequality.CodeQualityPlugin.Settings ++ cpdSettings ++ Seq(
    name := "BetterDocsIdeaPlugin",
    libraryDependencies ++= Dependencies.ideaPlugin,
    autoScalaLibrary := false,
    cpdLanguage := Language.Java,
    cpdMinimumTokens := 30,
    unmanagedBase := file(ideaLib.get)
    ))

  def coreSettings = betterDocsSettings ++ Seq(libraryDependencies ++= Dependencies.betterDocs)

  def betterDocsSettings =
    Defaults.coreDefaultSettings ++ Seq (
      name := "BetterDocs",
      organization := "com.betterdocs",
      version := "0.0.1-SNAPSHOT",
      scalaVersion := "2.11.6",
      scalacOptions := scalacOptionsList,
      resolvers += "apache special" at "https://repository.apache.org/content/repositories/orgapachespark-1083/",
     // retrieveManaged := true, // enable this if we need jars of dependencies.
      crossPaths := false,
      fork := true,
      javaOptions += "-Xmx3072m" // For running spark job.
      javacOptions ++= Seq("-source", "1.6"),
      javaOptions += "-Xmx2048m",
      javaOptions += "-XX:+HeapDumpOnOutOfMemoryError"

    )

}

object Dependencies {

  val spark = "org.apache.spark" %% "spark-core" % "1.3.1" // % "provided" Provided makes it not run through sbt run.
  val parserCombinator = "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.3"
  val scalaTest = "org.scalatest" %% "scalatest" % "2.2.4" % "test" 
  val slf4j = "org.slf4j" % "slf4j-log4j12" % "1.7.10"
  val javaparser = "com.github.javaparser" % "javaparser-core" % "2.0.0"

  val mllib = "org.apache.spark"  % "spark-mllib_2.11" % "1.2.1"

  val json4s = "org.json4s" %% "json4s-ast" % "3.2.10"
  val json4sJackson = "org.json4s" %% "json4s-jackson" % "3.2.10"
  val httpClient = "commons-httpclient" % "commons-httpclient" % "3.1"
  val config = "com.typesafe" % "config" % "1.2.1"
  val jgit = "org.eclipse.jgit" % "org.eclipse.jgit" % "3.7.0.201502260915-r"

  val betterDocs = Seq(spark, parserCombinator, scalaTest, slf4j, javaparser, json4s, config,
    json4sJackson, jgit, mllib)


  val ideaPlugin = Seq()
  // transitively uses
  // commons-io-2.4
  // commons-compress-1.4.1

}
