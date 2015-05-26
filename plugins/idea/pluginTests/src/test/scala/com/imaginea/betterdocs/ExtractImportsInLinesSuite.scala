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

package com.imaginea.betterdocs

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.JavaConversions._

class ExtractImportsInLinesSuite extends FunSuite with BeforeAndAfterAll {
  test("Extracted imports in lines should match the actual imports in lines.") {
    val lines = Set(
      "for String nextImport imports",
      "final Iterable String imports",
      "importsInLines add nextImport",
      "for String line lines",
      "if line contains nextImport substring nextImport lastIndexOf DOT 1",
      "public static final char DOT                                         ",
      "public class EditorDocOps                                         ",
      "Set String importsInLines new HashSet String                         ",
      "public final Set String importsInLines final Iterable String lines",
      "return importsInLines                                                ",
      "private static final String IMPORT import                            "
    )

    val imports = Set(
      "import com.intellij.openapi.editor.Document",
      "import com.intellij.openapi.editor.Editor",
      "import java.util.HashSet",
      "import java.util.Set"
    )
    val actual = new EditorDocOps().importsInLines(lines, imports)

    val expected = Set(
      "import java.util.HashSet",
      "import java.util.Set"
    )
    assert(actual.toSet === expected)
  }

}
