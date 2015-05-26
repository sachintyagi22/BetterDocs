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

public class RepoStarsJSON {
    private Query query;
    private int from;
    private int size;
    private List<Sort> sort;
    private Facets facets;

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

        public final void setBool(final Bool pBool) {
            this.bool = pBool;
        }

        public final Bool getBool() {
            return bool;
        }
    }

    public static class Bool {
        private List<Must> must;
        private List<Must> mustNot;
        private List<Must> should;

        public final void setMust(final List<Must> pMust) {
            this.must = pMust;
        }

        public final void setMustNot(final List<Must> pMustNot) {
            this.mustNot = pMustNot;
        }

        public final void setShould(final List<Must> pShould) {
            this.should = pShould;
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

        public final void setTerm(final Term pTerm) {
            this.term = pTerm;
        }

        public final Term getTerm() {
            return term;
        }
    }

    public static class Term {
        private int id;

        public final void setId(final int pId) {
            this.id = pId;
        }

        public final int getId() {
            return id;
        }
    }

    public static class Sort {
    }

    public static class Facets {
    }

    public final void setQuery(final Query pQuery) {
        this.query = pQuery;
    }

    public final void setFrom(final int pFrom) {
        this.from = pFrom;
    }

    public final void setSize(final int pSize) {
        this.size = pSize;
    }

    public final void setSort(final List<Sort> pSort) {
        this.sort = pSort;
    }

    public final Facets getFacets() {
        return facets;
    }

    public final void setFacets(final Facets pFacets) {
        this.facets = pFacets;
    }
}
