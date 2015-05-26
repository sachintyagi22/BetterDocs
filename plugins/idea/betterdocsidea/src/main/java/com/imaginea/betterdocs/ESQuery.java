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

    public final Query getQuery() {
        return query;
    }

    public final int getFrom() {
        return from;
    }

    public final int getSize() {
        return size;
    }

    public final List<Sort> getSort() {
        return sort;
    }

    public static class Query {
        private Bool bool;

        public final void setBool(final Bool pbool) {
            this.bool = pbool;
        }

        public final Bool getBool() {
            return bool;
        }
    }

    public static class Bool {
        private List<Must> must;
        private List<Must> mustNot;
        private List<Must> should;

        public final void setMust(final List<Must> pmust) {
            this.must = pmust;
        }

        public final void setMustNot(final List<Must> pmustNot) {
            this.mustNot = pmustNot;
        }

        public final void setShould(final List<Must> pshould) {
            this.should = pshould;
        }

        public final List<Must> getMust() {
            return must;
        }

        public final List<Must> getMustNot() {
            return mustNot;
        }

        public final List<Must> getShould() {
            return should;
        }
    }

    public static class Must {
        private Term term;

        public final void setTerm(final Term pterm) {
            this.term = pterm;
        }

        public final Term getTerm() {
            return term;
        }
    }

    public static class Term {
        private String importName;

        public final void setImportName(final String pimportName) {
            this.importName = pimportName;
        }

        public final String getImportName() {
            return importName;
        }
    }

    public static class Sort {
        private Score score;

        public final void setScore(final Score pscore) {
            this.score = pscore;
        }

        public final Score getScore() {
            return score;
        }
    }

    public static class Score {
        private String order;

        public final void setOrder(final String porder) {
            this.order = porder;
        }

        public final String getOrder() {
            return order;
        }
    }

    public final void setQuery(final Query pquery) {
        this.query = pquery;
    }

    public final void setFrom(final int pfrom) {
        this.from = pfrom;
    }

    public final void setSize(final int psize) {
        this.size = psize;
    }

    public final void setSort(final List<Sort> psort) {
        this.sort = psort;
    }
}
