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

import java.io.{StringWriter, InputStream}

import com.betterdocs.crawler.Repository
import org.apache.commons.io.IOUtils
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class ParserSuite extends FunSuite with BeforeAndAfterAll {

  test("Simple repo") {
    val r = RepoFileNameParser("repo~apache~zookeeper~160999~false~Java~trunk~789.zip")
    assert(r == Some(Repository("apache", 160999, "zookeeper", false, "Java", "trunk", 789)))
  }

  test("Names with special character") {
    val r = RepoFileNameParser("/home/dir~temp/repo~apache~zookeeper-lost~160999~false~Java" +
      "~=+-trunk/2.1~789.zip")
    assert(r == Some(Repository("apache", 160999, "zookeeper-lost", false, "Java", "=+-trunk/2.1",
      789)))
  }

  test("Branch name with version number only") {
    val r = RepoFileNameParser("repo~apache~zookeeper~160999~false~Java~2.1~789.zip")
    assert(r == Some(Repository("apache", 160999, "zookeeper", false, "Java", "2.1", 789)))
  }


  test("Branch name with tilde in it.") {
    val r = RepoFileNameParser("repo~apache~zookee~per~160999~false~Java~2.~1~789.zip")
    assert(r == None)
  }

  test("Branch name absent.") {
    val r = RepoFileNameParser("repo~apache~zookeeper~160999~false~Java~789.zip")
    assert(r == Some(Repository("apache", 160999, "zookeeper", false, "Java", "master", 789)))
  }

  test("Multiple valid repo names.") {
    val stream =
      Thread.currentThread().getContextClassLoader.getResourceAsStream("repo_names")
    val writer = new StringWriter()
    IOUtils.copy(stream, writer)
    val repoNames = writer.toString.split("\n").map { x =>
      (RepoFileNameParser(x), x)
    }
    assert(repoNames.filter(x => x._1 == None) === Seq())
  }
}

class MethodVisitorSuite extends FunSuite with BeforeAndAfterAll {

  import scala.collection.JavaConversions._

  val stream: InputStream =
    Thread.currentThread.getContextClassLoader.getResourceAsStream("TransportClient.java")

  val m: MethodVisitor = new MethodVisitor
  m.parse(stream)

  test("Verify method visitor includes lines with usages.") {

    val lines = m.getListOflineNumbersMap.flatMap(x => x.flatMap(_._2)).map(_.toInt)
      .toList.sorted.distinct

    assert(lines === List(74, 75, 79, 101, 102, 103, 105, 106, 108, 109, 112, 113, 114, 115, 117,
      118, 119, 120, 121, 123, 124, 125, 137, 138, 139, 141, 142, 144, 145, 148, 149, 150, 152,
      153, 154, 155, 156, 158, 159, 160, 172, 174, 177, 182, 187, 188, 189, 190, 191, 198, 203,
      204))
  }

  test("Should return a Map of import with methods and lineNumbers") {
    import java.io.InputStream
    val stream: InputStream =
      Thread.currentThread.getContextClassLoader.getResourceAsStream("TransportClient.java")

    import com.betterdocs.parser.MethodVisitor
    import com.betterdocs.parser.MethodVisitorHelper._
    val parser: MethodVisitor = new MethodVisitor
    parser.parse(stream)

    
    val testMap2 = Map(
      "com.google.common.util.concurrent.SettableFuture" -> 
      Map("set" -> List(177), "get" -> List(187), "setException" -> 
      List(182), "create" -> List(172)),
      "com.google.common.base.Objects" -> Map("toStringHelper" -> List(203)),
      "org.slf4j.Logger" -> Map("error" -> List(119, 125, 154, 160), 
          "debug" -> List(103), "trace" -> List(114, 139, 150)),
      "java.util.UUID" -> Map("randomUUID" -> List(141)),
      "org.apache.spark.network.util.NettyUtils" -> Map("getRemoteAddress" -> List(101, 137)),
      "io.netty.channel.ChannelFuture" -> Map("cause" -> List(118, 119, 123, 153, 154, 158),
        "isSuccess" -> List(112, 148)),
      "com.google.common.base.Preconditions" -> Map("checkNotNull" -> List(74, 75)),
      "io.netty.channel.Channel" -> Map("writeAndFlush" -> List(108, 144), "isActive" -> List(79),
        "isOpen" -> List(79), "close" -> List(121, 156, 198), "remoteAddress" -> List(204)),
      "java.util.concurrent.ExecutionException" -> Map("getCause" -> List(189)),
      "com.google.common.base.Throwables" -> Map("propagate" -> List(189, 191)))
    

    val resultTokens = getImportsWithMethodAndLineNumbers(parser);
    assert(resultTokens.size === testMap2.size)
    assert((testMap2.keySet diff resultTokens.keySet) === Set())
    assert((resultTokens.keySet diff testMap2.keySet) === Set())

    resultTokens.foreach {
      case (k, v) =>
        assert(resultTokens(k) === testMap2(k))
    }
  }

  override def afterAll() {
    stream.close()
  }
}

