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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

public class ESUtils {
    private static final String FILE_CONTENT = "fileContent";
    private static final String HITS = "hits";
    private static final String SOURCE = "_source";
    private static final String FILE = "file";
    private static final String TOKENS = "tokens";
    private static final String SOURCEFILE_SEARCH = "/sourcefile/_search?source=";
    private static final String REPOSITORY_SEARCH = "/repository/_search?source=";
    private static final String FAILED_HTTP_ERROR_CODE = "Failed : HTTP error code : ";
    private static final String USER_AGENT = "USER-AGENT";
    private static final String IDEA_PLUGIN = "Idea-Plugin";
    protected static final String UTF_8 = "UTF-8";
    private static final int HTTP_OK_STATUS = 200;
    private static final String REPO_ID = "repoId";
    private static final String STARGAZERS_COUNT = "stargazersCount";

    private static WindowObjects windowObjects = WindowObjects.getInstance();
    private JSONUtils jsonUtils = new JSONUtils();

    public final String getContentsForFile(final String file) {
        String esFileQueryJson = jsonUtils.getJsonForFileContent(file);
        String esFileResultJson = getESResultJson(esFileQueryJson,
                                    windowObjects.getEsURL() + SOURCEFILE_SEARCH);
        JsonArray hitsArray = getJsonElements(esFileResultJson);

        JsonObject hitObject = hitsArray.get(0).getAsJsonObject();
        JsonObject sourceObject = hitObject.getAsJsonObject(SOURCE);
        //Replacing \r as it's treated as bad end of line character
        String fileContent = sourceObject.getAsJsonPrimitive(FILE_CONTENT).
                                getAsString().replaceAll("\r", "");
        return fileContent;
    }

    public final Map<String, String> getFileTokens(final String esResultJson) {
        Map<String, String> fileTokenMap = new HashMap<String, String>();
        JsonArray hitsArray = getJsonElements(esResultJson);

        for (JsonElement hits : hitsArray) {
            JsonObject hitObject = hits.getAsJsonObject();
            JsonObject sourceObject = hitObject.getAsJsonObject(SOURCE);
            String fileName = sourceObject.getAsJsonPrimitive(FILE).getAsString();
            //Extracting repoIds for future use
            int repoId = sourceObject.getAsJsonPrimitive(REPO_ID).getAsInt();
            String project = getProjectName(fileName);
            if (!windowObjects.getRepoNameIdMap().containsKey(project)) {
                windowObjects.getRepoNameIdMap().put(project, repoId);
            }

            String tokens = sourceObject.get(TOKENS).toString();
            fileTokenMap.put(fileName, tokens);
        }
        return fileTokenMap;
    }

    protected final JsonArray getJsonElements(final String esResultJson) {
        JsonReader reader = new JsonReader(new StringReader(esResultJson));
        reader.setLenient(true);
        JsonElement jsonElement = new JsonParser().parse(reader);
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        JsonObject hitsObject = jsonObject.getAsJsonObject(HITS);
        return hitsObject.getAsJsonArray(HITS);
    }


    public final String getESResultJson(final String esQueryJson, final String url) {
        StringBuilder stringBuilder = new StringBuilder();
        try {
            HttpClient httpClient = new DefaultHttpClient();
            String encodedJson = URLEncoder.encode(esQueryJson, UTF_8);
            String esGetURL = url + encodedJson;

            HttpGet getRequest = new HttpGet(esGetURL);
            getRequest.setHeader(USER_AGENT, IDEA_PLUGIN);

            HttpResponse response = httpClient.execute(getRequest);
            if (response.getStatusLine().getStatusCode() != HTTP_OK_STATUS) {
                throw new RuntimeException(FAILED_HTTP_ERROR_CODE + url
                        + response.getStatusLine().getStatusCode());
            }

            BufferedReader bufferedReader = new BufferedReader(
                    new InputStreamReader((response.getEntity().getContent()), UTF_8));
            String output;
            while ((output = bufferedReader.readLine()) != null) {
                stringBuilder.append(output);
            }
            bufferedReader.close();
            httpClient.getConnectionManager().shutdown();
        } catch (IllegalStateException e) {
            e.printStackTrace();
            return RefreshAction.EMPTY_ES_URL;
        } catch (MalformedURLException e) {
            e.printStackTrace();
            return RefreshAction.EMPTY_ES_URL;
        } catch (IOException e) {
            e.printStackTrace();
            return RefreshAction.EMPTY_ES_URL;
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            return RefreshAction.EMPTY_ES_URL;
        }
        return stringBuilder.toString();
    }

    public final String getRepoStars(final String repoStarsJson) {
        String repoStarResultJson = getESResultJson(repoStarsJson,
                windowObjects.getEsURL() + REPOSITORY_SEARCH);
        JsonArray hitsArray = getJsonElements(repoStarResultJson);

        JsonObject hitObject = hitsArray.get(0).getAsJsonObject();
        JsonObject sourceObject = hitObject.getAsJsonObject(SOURCE);
        //Replacing \r as it's treated as bad end of line character
        String stars = sourceObject.getAsJsonPrimitive(STARGAZERS_COUNT).getAsString();
        return stars;
    }

    protected final String getProjectName(final String fileName) {
        //Project name is till 2nd '/'
        int startIndex = fileName.indexOf('/');
        int endIndex = fileName.indexOf('/', startIndex + 1);
        return fileName.substring(0, endIndex);
    }

    protected final String extractRepoStars(final String repoName, final int repoId) {
        String stars;
        if (windowObjects.getRepoStarsMap().containsKey(repoName)) {
            stars = windowObjects.getRepoStarsMap().get(repoName).toString();
        } else {
            String repoStarsJson = jsonUtils.getRepoStarsJSON(repoId);
            stars = getRepoStars(repoStarsJson);
            windowObjects.getRepoStarsMap().put(repoName, stars);
        }
        return stars;
    }
}
