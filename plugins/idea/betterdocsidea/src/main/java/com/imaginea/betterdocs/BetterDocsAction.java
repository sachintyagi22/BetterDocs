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
import com.intellij.icons.AllIcons;
import com.intellij.ide.util.PropertiesComponent;
import com.intellij.openapi.actionSystem.AnAction;
import com.intellij.openapi.actionSystem.AnActionEvent;
import com.intellij.openapi.actionSystem.DataKeys;
import com.intellij.openapi.application.Result;
import com.intellij.openapi.command.WriteCommandAction;
import com.intellij.openapi.editor.Document;
import com.intellij.openapi.editor.Editor;
import com.intellij.openapi.editor.FoldRegion;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.ui.Messages;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.swing.JTree;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.MutableTreeNode;
import javax.swing.tree.TreeNode;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.jetbrains.annotations.NotNull;

public class BetterDocsAction extends AnAction {
    private static final String IMPORT_NAME = "importName";
    private static final String LINE_NUMBERS = "lineNumbers";
    private static final String BETTER_DOCS = "BetterDocs";
    private static final String HITS = "hits";
    private static final String SOURCE = "_source";
    private static final String FILE_CONTENT = "fileContent";
    private static final String IMPORT = "import ";
    private static final String SORT_ORDER = "desc";
    private static final String FILE = "file";
    private static final String TOKENS = "tokens";
    private static final String CUSTOM_TOKENS_IMPORT_NAME = "custom.tokens.importName";
    private static final char DOT = '.';
    private static final String ILLEGAL_FORMAT = "Please Provide valid numbers for distance and size in settings. Using default values for now";
    private static final String INFO = "Info";
    private static final String EMPTY_ES_URL = "Please set/modify proper esURL in idea settings";
    public static final String ES_URL = "esURL";
    public static final String DISTANCE = "distance";
    public static final String SIZE = "size";
    private static final String BETTERDOCS_SEARCH = "/betterdocs/_search?source=";
    private static final String SOURCEFILE_SEARCH = "/sourcefile/_search?source=";
    private static final String FAILED_HTTP_ERROR_CODE = "Failed : HTTP error code : ";
    public static final String ES_URL_DEFAULT = "http://labs.imaginea.com/betterdocs";
    public static final int DISTANCE_DEFAULT_VALUE = 10;
    public static final int SIZE_DEFAULT_VALUE = 30;
    private static final String USER_AGENT = "USER-AGENT";
    private static final String IDEA_PLUGIN = "Idea-Plugin";
    private static final String UTF_8 = "UTF-8";


    private Project project;
    private JTree jTree;
    private Editor windowEditor;

    private int distance;
    private int size;
    private String esURL;

    public void setWindowEditor(Editor windowEditor) {
        this.windowEditor = windowEditor;
    }

    public void setTree(JTree jTree) {
        this.jTree = jTree;
    }

    public BetterDocsAction() {
        super(BETTER_DOCS, BETTER_DOCS, AllIcons.Actions.Refresh);
    }

    @Override
    public void actionPerformed(@NotNull AnActionEvent anActionEvent) {
        setProject(anActionEvent.getProject());
        PropertiesComponent propertiesComponent = PropertiesComponent.getInstance();
        try {
            this.distance = propertiesComponent.getOrInitInt(DISTANCE, DISTANCE_DEFAULT_VALUE);
            this.size = propertiesComponent.getOrInitInt(SIZE, SIZE_DEFAULT_VALUE);
            this.esURL = propertiesComponent.getValue(ES_URL, ES_URL_DEFAULT);
        } catch (NumberFormatException ne) {
            this.distance = DISTANCE_DEFAULT_VALUE;
            this.size = SIZE_DEFAULT_VALUE;
            Messages.showInfoMessage(String.format(ILLEGAL_FORMAT), INFO);
        }

        try {
            runAction(anActionEvent);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    private static MutableTreeNode getNodes(String projectName, Iterable<CodeInfo> codeInfoCollection) {
        DefaultMutableTreeNode node = new DefaultMutableTreeNode(projectName);
        Collection<String> fileNameSet = new HashSet<String>();
        for (CodeInfo codeInfo : codeInfoCollection) {
            if (!fileNameSet.contains(codeInfo.getFileName())) {
                node.add(new DefaultMutableTreeNode(codeInfo));
                fileNameSet.add(codeInfo.getFileName());
            }
        }
        return node;
    }

    public void runAction(final AnActionEvent e) throws IOException {
        final Editor projectEditor = DataKeys.EDITOR.getData(e.getDataContext());

        if (projectEditor != null) {

            Set<String> imports = getImports(projectEditor.getDocument());
            Set<String> lines = getLines(projectEditor, projectEditor.getDocument());
            Set<String> importsInLines = importsInLines(lines, imports);

            DefaultTreeModel model = (DefaultTreeModel) jTree.getModel();
            DefaultMutableTreeNode root = (DefaultMutableTreeNode) model.getRoot();
            root.removeAllChildren();

            if (!importsInLines.isEmpty()) {
                jTree.setVisible(true);
                String esQueryJson = getESQueryJson(importsInLines);
                String esResultJson = getESResultJson(esQueryJson, esURL + BETTERDOCS_SEARCH);

                if (!esResultJson.equals(EMPTY_ES_URL)) {
                    Map<String, String> fileTokensMap = getFileTokens(esResultJson);
                    Map<String, ArrayList<CodeInfo>> projectNodes = new HashMap<String, ArrayList<CodeInfo>>();

                    updateProjectNodes(imports, fileTokensMap, projectNodes);
                    updateRoot(root, projectNodes);

                    model.reload(root);
                    jTree.addTreeSelectionListener(getTreeSelectionListener(root));
                } else {
                    Messages.showInfoMessage(EMPTY_ES_URL, INFO);
                }
            } else {
                jTree.updateUI();
            }
        }
    }

    private void updateProjectNodes(Collection<String> imports, Map<String, String> fileTokensMap, Map<String, ArrayList<CodeInfo>> projectNodes) {
        for (Map.Entry<String, String> entry : fileTokensMap.entrySet()) {
            String fileName = entry.getKey();
            String tokens = entry.getValue();

            List<Integer> lineNumbers = getLineNumbers(imports, tokens);
            String contents = getContentsForFile(fileName);
            CodeInfo codeInfo = new CodeInfo(fileName, lineNumbers, contents);

            //Taking projectName as name till 2nd '/'
            int startIndex = fileName.indexOf('/');
            int endIndex = fileName.indexOf('/', startIndex + 1);

            String projectName = fileName.substring(0, endIndex);

            if (projectNodes.containsKey(projectName)) {
                projectNodes.get(projectName).add(codeInfo);
            } else {
                projectNodes.put(projectName, new ArrayList<CodeInfo>(Collections.singletonList(codeInfo)));
            }
        }
    }

    private static DefaultMutableTreeNode updateRoot(DefaultMutableTreeNode root, Map<String, ArrayList<CodeInfo>> projectNodes) {
        for (Map.Entry<String, ArrayList<CodeInfo>> entry : projectNodes.entrySet()) {
            root.add(getNodes(entry.getKey(), entry.getValue()));
        }
        return root;
    }

    private TreeSelectionListener getTreeSelectionListener(final TreeNode root) {
        return new TreeSelectionListener() {
            @Override
            public void valueChanged(TreeSelectionEvent treeSelectionEvent) {
                DefaultMutableTreeNode selectedNode = (DefaultMutableTreeNode)
                        jTree.getLastSelectedPathComponent();

                if (selectedNode != null && selectedNode.isLeaf() && root.getChildCount() > 0) {
                    final CodeInfo codeInfo = (CodeInfo) selectedNode.getUserObject();
                    final Document windowEditorDocument = windowEditor.getDocument();

                    writeToDocument(codeInfo, windowEditorDocument);

                    final List<Integer> linesForFolding = codeInfo.getLineNumbers();
                    linesForFolding.add(windowEditorDocument.getLineCount() + 1);
                    java.util.Collections.sort(linesForFolding);
                    addFoldings(windowEditorDocument, linesForFolding);
                }
            }
        };
    }

    private void writeToDocument(final CodeInfo codeInfo, final Document windowEditorDocument) {
        new WriteCommandAction(project) {
            @Override
            protected void run(@NotNull Result result) throws Throwable {
                windowEditorDocument.setReadOnly(false);
                windowEditorDocument.setText(codeInfo.getContents());
                windowEditorDocument.setReadOnly(true);
            }
        }.execute();

    }

    private void addFoldings(final Document windowEditorDocument, final Iterable<Integer> linesForFolding) {
        windowEditor.getFoldingModel().runBatchFoldingOperation(new Runnable() {
            @Override
            public void run() {
                int prevLine = 0;
                cleanFoldingRegions(windowEditor);

                for (int line : linesForFolding) {
                    int currentLine = line - 1;
                    if (prevLine < windowEditorDocument.getLineCount()) {

                        int startOffset = windowEditorDocument.getLineStartOffset(prevLine);
                        int endOffset = windowEditorDocument.getLineEndOffset(currentLine - 1);

                        if (startOffset < endOffset && windowEditor.getFoldingModel() != null) {
                            windowEditor.getFoldingModel()
                                    .addFoldRegion(startOffset, endOffset, "...")
                                    .setExpanded(false);
                        }
                        prevLine = currentLine + 1;
                    }
                }
            }
        });
    }

    private static void cleanFoldingRegions(Editor windowEditor) {
        FoldRegion[] foldRegions = windowEditor.getFoldingModel().getAllFoldRegions();
        for (FoldRegion currentRegion : foldRegions) {
            windowEditor.getFoldingModel().removeFoldRegion(currentRegion);
        }
    }

    private List<Integer> getLineNumbers(Collection<String> imports, String tokens) {
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


    private String getContentsForFile(String file) {
        String esFileQueryJson = getJsonForFileContent(file);
        String esFileResultJson = getESResultJson(esFileQueryJson, esURL + SOURCEFILE_SEARCH);

        JsonReader reader = new JsonReader(new StringReader(esFileResultJson));
        reader.setLenient(true);
        JsonElement jsonElement = new JsonParser().parse(reader);
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        JsonObject hitsObject = jsonObject.getAsJsonObject(HITS);
        JsonArray hitsArray = hitsObject.getAsJsonArray(HITS);
        JsonObject hitObject = hitsArray.get(0).getAsJsonObject();
        JsonObject sourceObject = hitObject.getAsJsonObject(SOURCE);
        //Replacing \r as it's treated as bad end of line character
        String fileContent = sourceObject.getAsJsonPrimitive(FILE_CONTENT).getAsString().replaceAll("\r", "");
        return fileContent;
    }

    public void setProject(Project project) {
        this.project = project;
    }

    private static Set<String> importsInLines(Iterable<String> lines, Iterable<String> imports) {
        Set<String> importsInLines = new HashSet<String>();

        for (String line : lines) {
            for (String nextImport : imports) {
                if (line.contains(nextImport.substring(nextImport.lastIndexOf(DOT) + 1))) {
                    importsInLines.add(nextImport);
                }
            }
        }
        return importsInLines;
    }

    private Set<String> getLines(Editor projectEditor, Document document) {
        Set<String> lines = new HashSet<String>();
        int startLine;
        int endLine;

        if (projectEditor.getSelectionModel().hasSelection()) {
            startLine = document.getLineNumber(projectEditor.getSelectionModel().getSelectionStart());
            endLine = document.getLineNumber(projectEditor.getSelectionModel().getSelectionEnd());
        } else {
            int currentLine = document.getLineNumber(projectEditor.getCaretModel().getOffset());
            startLine = currentLine - distance >= 0 ? currentLine - distance : 0;
            endLine = currentLine + distance <= document.getLineCount() - 1 ? currentLine + distance : document.getLineCount() - 1;
        }

        for (int i = startLine; i <= endLine; i++) {
            String line = document.getCharsSequence().subSequence(document.getLineStartOffset(i), document.getLineEndOffset(i)).toString();
            if (!line.contains(IMPORT)) {
                lines.add(line);
            }
        }
        return lines;
    }

    private static Set<String> getImports(Document document) {
        int startLine = 0;
        int endLine = document.getLineCount() - 1;
        Set<String> imports = new HashSet<String>();
        for (int i = startLine; i <= endLine; i++) {
            String line = document.getCharsSequence().subSequence(document.getLineStartOffset(i), document.getLineEndOffset(i) + document.getLineSeparatorLength(i)).toString();
            if (line.contains(IMPORT) && !line.contains("*")) {
                imports.add(line.replace(IMPORT, "").replace(";", "").trim());
            }
        }
        return imports;
    }

    private String getESQueryJson(Set<String> importsInLines) {
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


    private static String getESResultJson(String esQueryJson, String url) {
        StringBuilder stringBuilder = new StringBuilder();
        try {
            HttpClient httpClient = new DefaultHttpClient();
            String encodedJson = URLEncoder.encode(esQueryJson, UTF_8);
            String esGetURL = url + encodedJson;

            HttpGet getRequest = new HttpGet(esGetURL);
            getRequest.setHeader(USER_AGENT, IDEA_PLUGIN);

            HttpResponse response = httpClient.execute(getRequest);
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new RuntimeException(FAILED_HTTP_ERROR_CODE + url +
                        response.getStatusLine().getStatusCode());
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
            return EMPTY_ES_URL;
        } catch (MalformedURLException e) {
            e.printStackTrace();
            return EMPTY_ES_URL;
        } catch (IOException e) {
            e.printStackTrace();
            return EMPTY_ES_URL;
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            return EMPTY_ES_URL;
        }
        return stringBuilder.toString();
    }

    private static Map<String, String> getFileTokens(String esResultJson) {
        Map<String, String> fileTokenMap = new HashMap<String, String>();
        JsonReader reader = new JsonReader(new StringReader(esResultJson));
        reader.setLenient(true);
        JsonElement jsonElement = new JsonParser().parse(reader);
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        JsonObject hitsObject = jsonObject.getAsJsonObject(HITS);
        JsonArray hitsArray = hitsObject.getAsJsonArray(HITS);

        for (JsonElement hits : hitsArray) {
            JsonObject hitObject = hits.getAsJsonObject();
            JsonObject sourceObject = hitObject.getAsJsonObject(SOURCE);
            String fileName = sourceObject.getAsJsonPrimitive(FILE).getAsString();
            String tokens = sourceObject.get(TOKENS).toString();
            fileTokenMap.put(fileName, tokens);
        }
        return fileTokenMap;
    }

    private static String getJsonForFileContent(String fileName) {
        ESFileContent esFileContent = new ESFileContent();
        ESFileContent.Query query = new ESFileContent.Query();
        esFileContent.setQuery(query);
        ESFileContent.Term term = new ESFileContent.Term();
        query.setTerm(term);
        term.setFileName(fileName);
        Gson gson = new Gson();
        return gson.toJson(esFileContent);
    }
}
