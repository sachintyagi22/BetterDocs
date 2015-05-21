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

import com.intellij.icons.AllIcons;
import com.intellij.openapi.actionSystem.ActionManager;
import com.intellij.openapi.actionSystem.DefaultActionGroup;
import com.intellij.openapi.editor.Document;
import com.intellij.openapi.editor.Editor;
import com.intellij.openapi.editor.EditorFactory;
import com.intellij.openapi.fileTypes.FileTypeManager;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.wm.ToolWindow;
import com.intellij.openapi.wm.ToolWindowFactory;
import com.intellij.ui.JBColor;
import com.intellij.ui.components.JBScrollPane;
import com.intellij.ui.components.JBTabbedPane;
import com.intellij.ui.treeStructure.Tree;
import java.awt.Dimension;
import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;

public class MainWindow implements ToolWindowFactory {

    private static final String PROJECTS = "Projects";
    protected static final String JAVA = "java";
    private static final double DIVIDER_LOCATION = 0.2;
    private static final String MAIN_PANE = "Main Pane";
    private static final String CODE_PANE = "Code Pane";
    private static final int EDITOR_SCROLL_PANE_WIDTH = 200;
    private static final int EDITOR_SCROLL_PANE_HEIGHT = 300;
    private static final String BETTERDOCS = "BetterDocs";
    private static final int UNIT_INCREMENT = 16;
    private WindowEditorOps windowEditorOps = new WindowEditorOps();
    private Editor windowEditor;

    @Override
    public final void createToolWindowContent(final Project project, final ToolWindow toolWindow) {
        toolWindow.setIcon(AllIcons.Toolwindows.Documentation);
        DefaultMutableTreeNode root = new DefaultMutableTreeNode(PROJECTS);

        JTree jTree = new Tree(root);
        jTree.setRootVisible(false);
        jTree.setAutoscrolls(true);

        Document document = EditorFactory.getInstance().createDocument("");
        windowEditor = EditorFactory.getInstance().
                        createEditor(document, project, FileTypeManager.getInstance().
                                getFileTypeByExtension(JAVA), false);

        RefreshAction refreshAction = new RefreshAction();
        EditSettingsAction editSettingsAction = new EditSettingsAction();
        ExpandProjectTreeAction expandProjectTreeAction = new ExpandProjectTreeAction();
        CollapseProjectTreeAction collapseProjectTreeAction = new CollapseProjectTreeAction();
        WindowObjects windowObjects = WindowObjects.getInstance();

        windowObjects.setTree(jTree);
        windowObjects.setWindowEditor(windowEditor);

        DefaultActionGroup group = new DefaultActionGroup();
        group.add(refreshAction);
        group.addSeparator();
        group.add(expandProjectTreeAction);
        group.add(collapseProjectTreeAction);
        group.addSeparator();
        group.add(editSettingsAction);
        JComponent toolBar = ActionManager.getInstance().
                createActionToolbar(BETTERDOCS, group, true).
                getComponent();
        toolBar.setBorder(BorderFactory.createCompoundBorder());

        toolBar.setMaximumSize(new Dimension(Integer.MAX_VALUE, toolBar.getMinimumSize().height));


        JBScrollPane jTreeScrollPane = new JBScrollPane();
        jTreeScrollPane.getViewport().setBackground(JBColor.white);
        jTreeScrollPane.setViewportView(new JLabel(RefreshAction.HELP_MESSAGE));
        jTreeScrollPane.setAutoscrolls(true);
        jTreeScrollPane.setBackground(JBColor.white);
        windowObjects.setJTreeScrollPane(jTreeScrollPane);

        final JSplitPane jSplitPane = new JSplitPane(
                        JSplitPane.VERTICAL_SPLIT, windowEditor.getComponent(), jTreeScrollPane);
        jSplitPane.setResizeWeight(DIVIDER_LOCATION);

        JPanel editorPanel = new JPanel();
        editorPanel.setOpaque(true);
        editorPanel.setBackground(JBColor.white);
        editorPanel.setLayout(new BoxLayout(editorPanel, BoxLayout.Y_AXIS));

        final JBScrollPane editorScrollPane = new JBScrollPane();
        editorScrollPane.getViewport().setBackground(JBColor.white);
        editorScrollPane.setViewportView(editorPanel);
        editorScrollPane.setAutoscrolls(true);
        editorScrollPane.setPreferredSize(new Dimension(EDITOR_SCROLL_PANE_WIDTH,
                EDITOR_SCROLL_PANE_HEIGHT));
        editorScrollPane.getVerticalScrollBar().setUnitIncrement(UNIT_INCREMENT);

        windowObjects.setPanel(editorPanel);

        final JTabbedPane jTabbedPane = new JBTabbedPane();
        jTabbedPane.add(MAIN_PANE, jSplitPane);
        jTabbedPane.add(CODE_PANE, editorScrollPane);
        refreshAction.setJTabbedPane(jTabbedPane);

        final JPanel mainPanel = new JPanel();
        mainPanel.setLayout((new BoxLayout(mainPanel, BoxLayout.Y_AXIS)));
        mainPanel.add(toolBar);
        mainPanel.add(jTabbedPane);

        toolWindow.getComponent().getParent().add(mainPanel);
        //Dispose the editor once it's no longer needed
        windowEditorOps.releaseEditor(project, windowEditor);
    }
}
