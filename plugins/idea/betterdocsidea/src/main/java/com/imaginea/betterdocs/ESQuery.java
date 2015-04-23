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

import java.util.List;

public class ESQuery {
    private Query query;
    private int from;
    private int size;
    private List<Sort> sort;

    public Query getQuery() {
        return query;
    }

    public int getFrom() {
        return from;
    }

    public int getSize() {
        return size;
    }

    public List<Sort> getSort() {
        return sort;
    }

    public static class Query {
        private Bool bool;

        public void setBool(Bool bool) {
            this.bool = bool;
        }

        public Bool getBool() {
            return bool;
        }
    }

    public static class Bool {
        private List<Must> must;
        private List<Must> mustNot;
        private List<Must> should;

        public void setMust(List<Must> must) {
            this.must = must;
        }

        public void setMustNot(List<Must> mustNot) {
            this.mustNot = mustNot;
        }

        public void setShould(List<Must> should) {
            this.should = should;
        }

        public List<Must> getMust() {
            return must;
        }

        public List<Must> getMustNot() {
            return mustNot;
        }

        public List<Must> getShould() {
            return should;
        }
    }

    public static class Must {
        private Term term;

        public void setTerm(Term term) {
            this.term = term;
        }

        public Term getTerm() {
            return term;
        }
    }

    public static class Term {
        private String importName;

        public void setImportName(String importName) {
            this.importName = importName;
        }

        public String getImportName() {
            return importName;
        }
    }

    public static class Sort {
        private Score score;

        public void setScore(Score score) {
            this.score = score;
        }

        public Score getScore() {
            return score;
        }
    }

    public static class Score {
        private String order;

        public void setOrder(String order) {
            this.order = order;
        }

        public String getOrder() {
            return order;
        }
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public void setFrom(int from) {
        this.from = from;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public void setSort(List<Sort> sort) {
        this.sort = sort;
    }
}
