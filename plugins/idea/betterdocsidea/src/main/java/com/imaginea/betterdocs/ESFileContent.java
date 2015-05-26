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

package com.imaginea.betterdocs;

public class ESFileContent {
    public final Query getQuery() {
        return query;
    }

    private Query query;

    public static class Query {
        private Term term;

        public final void setTerm(final Term pterm) {
            this.term = pterm;
        }

        public final Term getTerm() {
            return term;
        }
    }

    public static class Term {
        private String fileName;

        public final void setFileName(final String pfileName) {
            this.fileName = pfileName;
        }

        public final String getFileName() {
            return fileName;
        }
    }

    public final void setQuery(final Query pquery) {
        this.query = pquery;
    }
}
