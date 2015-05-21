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

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class JSONUtils {
    private static final String CUSTOM_TOKENS_IMPORT_NAME = "custom.tokens.importName";
    private static final String IMPORT_NAME = "importName";
    private static final String LINE_NUMBERS = "lineNumbers";
    private static final String SORT_ORDER = "desc";
    private static final String ID = "id";
    private static final String TYPEREPOSITORY_ID = "typerepository.id";

    public final String getJsonForFileContent(final String fileName) {
        ESFileContent esFileContent = new ESFileContent();
        ESFileContent.Query query = new ESFileContent.Query();
        esFileContent.setQuery(query);
        ESFileContent.Term term = new ESFileContent.Term();
        query.setTerm(term);
        term.setFileName(fileName);
        Gson gson = new Gson();
        return gson.toJson(esFileContent);
    }

    public final String getRepoStarsJSON(final int repoId) {
        RepoStarsJSON repoStarsJSON = new RepoStarsJSON();
        RepoStarsJSON.Query query = new RepoStarsJSON.Query();
        repoStarsJSON.setQuery(query);
        repoStarsJSON.setFrom(0);
        repoStarsJSON.setSize(1);

        List<RepoStarsJSON.Sort> sortList = new ArrayList<RepoStarsJSON.Sort>();
        repoStarsJSON.setSort(sortList);

        RepoStarsJSON.Facets facets = new RepoStarsJSON.Facets();
        repoStarsJSON.setFacets(facets);

        RepoStarsJSON.Bool bool = new RepoStarsJSON.Bool();
        query.setBool(bool);

        List<RepoStarsJSON.Must> mustList = new ArrayList<RepoStarsJSON.Must>();

        RepoStarsJSON.Must must = new RepoStarsJSON.Must();
        bool.setMust(mustList);
        bool.setMustNot(new ArrayList<RepoStarsJSON.Must>());
        bool.setShould(new ArrayList<RepoStarsJSON.Must>());
        RepoStarsJSON.Term term = new RepoStarsJSON.Term();
        must.setTerm(term);
        term.setId(repoId);
        mustList.add(must);

        Gson gson = new Gson();
        return gson.toJson(repoStarsJSON).replaceAll(ID, TYPEREPOSITORY_ID);
    }

    public final String getESQueryJson(final Set<String> importsInLines, final int size) {
        ESQuery esQuery = new ESQuery();
        ESQuery.Query query = new ESQuery.Query();
        esQuery.setQuery(query);
        esQuery.setFrom(0);
        esQuery.setSize(size);

        List<ESQuery.Sort> sortList = new ArrayList<ESQuery.Sort>();

        ESQuery.Sort sort = new ESQuery.Sort();

        ESQuery.Score score = new ESQuery.Score();
        score.setOrder(SORT_ORDER);

        sort.setScore(score);

        sortList.add(sort);
        esQuery.setSort(sortList);

        ESQuery.Bool bool = new ESQuery.Bool();
        query.setBool(bool);

        List<ESQuery.Must> mustList = new ArrayList<ESQuery.Must>();

        ESQuery.Must must;
        ESQuery.Term term;

        for (String nextImport : importsInLines) {
            must = new ESQuery.Must();
            bool.setMust(mustList);
            bool.setMustNot(new ArrayList<ESQuery.Must>());
            bool.setShould(new ArrayList<ESQuery.Must>());
            term = new ESQuery.Term();
            must.setTerm(term);
            term.setImportName(nextImport);
            mustList.add(must);
        }

        Gson gson = new Gson();
        return gson.toJson(esQuery).replaceAll(IMPORT_NAME, CUSTOM_TOKENS_IMPORT_NAME);
    }

    public final List<Integer> getLineNumbers(final Collection<String> imports,
                                              final String tokens) {
        List<Integer> lineNumbers = new ArrayList<Integer>();
        JsonReader reader = new JsonReader(new StringReader(tokens));
        reader.setLenient(true);
        JsonArray tokensArray = new JsonParser().parse(reader).getAsJsonArray();

        for (JsonElement token : tokensArray) {
            JsonObject jObject = token.getAsJsonObject();
            String importName = jObject.getAsJsonPrimitive(IMPORT_NAME).getAsString();
            if (imports.contains(importName)) {
                JsonArray lineNumbersArray = jObject.getAsJsonArray(LINE_NUMBERS);
                for (JsonElement lineNumber : lineNumbersArray) {
                    lineNumbers.add(lineNumber.getAsInt());
                }
            }
        }
        return lineNumbers;
    }
}
