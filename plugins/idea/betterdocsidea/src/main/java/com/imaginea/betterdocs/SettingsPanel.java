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

import com.intellij.ide.util.PropertiesComponent;
import com.intellij.openapi.options.Configurable;
import com.jgoodies.forms.layout.CellConstraints;
import com.jgoodies.forms.layout.FormLayout;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;
import org.jetbrains.annotations.Nls;
import org.jetbrains.annotations.Nullable;

public class SettingsPanel implements Configurable {
    protected static final String BETTER_DOCS_SETTINGS = "BetterDocs Settings";
    private static final String COLUMN_SPECS = "pref, pref:grow";
    private static final String ROW_SPECS = "pref, pref, pref, pref, pref";
    private static final String ELASTIC_SEARCH_URL = "Elastic Search URL";
    private static final String RESULTS_SIZE = "Results size";
    private static final String DISTANCE_FROM_CURSOR = "Distance from cursor";
    private static final String EXCLUDE_IMPORT_LIST = "Exclude imports";
    private static final String HELP_TEXT =
        "Please enter comma separated regex"
            + "(e.g. java.util.[A-Z][a-z0-9]*, org.slf4j.Logger)";

    private JTextField excludeImportsText;

    private JTextField sizeText;
    private JTextField distanceText;
    private JTextField esURLText;

    @Nls
    @Override
    public final String getDisplayName() {
        return BETTER_DOCS_SETTINGS;
    }

    @Nullable
    @Override
    public final String getHelpTopic() {
        //Need to provide URL for plugin in JetBrain's website
        return "";
    }

    @Nullable
    @Override
    public final JComponent createComponent() {
        FormLayout formLayout = new FormLayout(
                COLUMN_SPECS,
                ROW_SPECS);

        PropertiesComponent propertiesComponent = PropertiesComponent.getInstance();

        CellConstraints cc = new CellConstraints();

        JLabel esURL = new JLabel(ELASTIC_SEARCH_URL);
        esURL.setVisible(true);

        JLabel size = new JLabel(RESULTS_SIZE);
        size.setVisible(true);

        JLabel distance = new JLabel(DISTANCE_FROM_CURSOR);
        distance.setVisible(true);

        JLabel excludeImports = new JLabel(EXCLUDE_IMPORT_LIST);
        excludeImports.setVisible(true);

        JLabel helpText = new JLabel(HELP_TEXT);
        helpText.setVisible(true);

        esURLText = new JTextField();
        esURLText.setEditable(true);
        esURLText.setVisible(true);

        if (propertiesComponent.isValueSet(RefreshAction.ES_URL)) {
            esURLText.setText(propertiesComponent.getValue(RefreshAction.ES_URL));
        } else {
            esURLText.setText(RefreshAction.ES_URL_DEFAULT);
        }

        sizeText = new JTextField();
        sizeText.setEditable(true);
        sizeText.setVisible(true);

        sizeText.setText(propertiesComponent.getValue(RefreshAction.SIZE,
                            String.valueOf(RefreshAction.SIZE_DEFAULT_VALUE)));

        distanceText = new JTextField();
        distanceText.setEditable(true);
        distanceText.setVisible(true);

        distanceText.setText(propertiesComponent.getValue(RefreshAction.DISTANCE,
                            String.valueOf(RefreshAction.DISTANCE_DEFAULT_VALUE)));

        excludeImportsText = new JTextField();
        excludeImportsText.setEditable(true);
        excludeImportsText.setVisible(true);

        if (propertiesComponent.isValueSet(RefreshAction.EXCLUDE_IMPORT_LIST)) {
            excludeImportsText.setText(propertiesComponent.getValue(RefreshAction.
                                       EXCLUDE_IMPORT_LIST));
        }
        JPanel jPanel = new JPanel(formLayout);
        jPanel.add(esURL, cc.xy(1, 3));
        jPanel.add(esURLText, cc.xy(2, 3));
        jPanel.add(size, cc.xy(1, 2));
        jPanel.add(sizeText, cc.xy(2, 2));
        jPanel.add(distance, cc.xy(1, 1));
        jPanel.add(distanceText, cc.xy(2, 1));
        jPanel.add(excludeImports, cc.xy(1, 4));
        jPanel.add(excludeImportsText, cc.xy(2, 4));
        jPanel.add(helpText, cc.xy(2, 5));

        return jPanel;
    }

    @Override
    public final boolean isModified() {
        return true;
    }

    @Override
    public final void apply() {
        PropertiesComponent propertiesComponent = PropertiesComponent.getInstance();
        String esURLValue = esURLText.getText();
        String sizeValue = sizeText.getText();
        String distanceValue = distanceText.getText();
        String excludeImportsValues = excludeImportsText.getText();
        propertiesComponent.setValue(RefreshAction.ES_URL, esURLValue);
        propertiesComponent.setValue(RefreshAction.SIZE, sizeValue);
        propertiesComponent.setValue(RefreshAction.DISTANCE, distanceValue);
        propertiesComponent.setValue(RefreshAction.EXCLUDE_IMPORT_LIST,
                                      excludeImportsValues);
    }

    @Override
    public final void reset() {
        PropertiesComponent propertiesComponent = PropertiesComponent.getInstance();
        esURLText.setText(propertiesComponent.
                            getValue(RefreshAction.ES_URL,
                                    RefreshAction.ES_URL_DEFAULT));
        sizeText.setText(propertiesComponent.
                            getValue(RefreshAction.SIZE,
                                    String.valueOf(RefreshAction.SIZE_DEFAULT_VALUE)));
        distanceText.setText(propertiesComponent.
                            getValue(RefreshAction.DISTANCE,
                                    String.valueOf(RefreshAction.DISTANCE_DEFAULT_VALUE)));

        if (propertiesComponent.isValueSet(RefreshAction.EXCLUDE_IMPORT_LIST)) {
            excludeImportsText.setText(propertiesComponent.getValue(RefreshAction.
                                       EXCLUDE_IMPORT_LIST));
        }
    }

    @Override
    public void disposeUIResources() {

    }
}
