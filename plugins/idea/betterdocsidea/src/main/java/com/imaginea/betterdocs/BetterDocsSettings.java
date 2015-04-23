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
import com.intellij.openapi.options.ConfigurationException;
import com.jgoodies.forms.layout.CellConstraints;
import com.jgoodies.forms.layout.FormLayout;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;
import org.jetbrains.annotations.Nls;
import org.jetbrains.annotations.Nullable;

public class BetterDocsSettings implements Configurable {
    private static final String BETTER_DOCS_SETTINGS = "BetterDocs Settings";
    private static final String COLUMNS = "pref, pref:grow";
    private static final String COLUMN_SPECS = COLUMNS;
    private static final String ROW_SPECS = "pref, pref, pref";
    private static final String ELASTIC_SEARCH_URL = "Elastic Search URL";
    private static final String RESULTS_SIZE = "Results size";
    private static final String DISTANCE_FROM_CURSOR = "Distance from cursor";

    private JTextField sizeText;
    private JTextField distanceText;
    private JTextField esURLText;

    @Nls
    @Override
    public String getDisplayName() {
        return BETTER_DOCS_SETTINGS;
    }

    @Nullable
    @Override
    public String getHelpTopic() {
        //TODO: Provide URL for HelpTopic in JetBrains website about plugin
        return "";
    }

    @Nullable
    @Override
    public JComponent createComponent() {
        FormLayout layout = new FormLayout(
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

        esURLText = new JTextField();
        esURLText.setEditable(true);
        esURLText.setVisible(true);

        if (propertiesComponent.isValueSet(BetterDocsAction.ES_URL)) {
            esURLText.setText(propertiesComponent.getValue(BetterDocsAction.ES_URL));
        } else {
            esURLText.setText(BetterDocsAction.ES_URL_DEFAULT);
        }

        sizeText = new JTextField();
        sizeText.setEditable(true);
        sizeText.setVisible(true);

        sizeText.setText(propertiesComponent.getValue(BetterDocsAction.SIZE, String.valueOf(BetterDocsAction.SIZE_DEFAULT_VALUE)));

        distanceText = new JTextField();
        distanceText.setEditable(true);
        distanceText.setVisible(true);

        distanceText.setText(propertiesComponent.getValue(BetterDocsAction.DISTANCE, String.valueOf(BetterDocsAction.DISTANCE_DEFAULT_VALUE)));

        JPanel jPanel = new JPanel(layout);
        jPanel.add(esURL, cc.xy(1, 3));
        jPanel.add(esURLText, cc.xy(2, 3));
        jPanel.add(size, cc.xy(1, 2));
        jPanel.add(sizeText, cc.xy(2, 2));
        jPanel.add(distance, cc.xy(1, 1));
        jPanel.add(distanceText, cc.xy(2, 1));

        return jPanel;
    }

    @Override
    public boolean isModified() {
        return true;
    }

    @Override
    public void apply() throws ConfigurationException {
        PropertiesComponent propertiesComponent = PropertiesComponent.getInstance();
        String esURLValue = esURLText.getText();
        String sizeValue = sizeText.getText();
        String distanceValue = distanceText.getText();
        propertiesComponent.setValue(BetterDocsAction.ES_URL, esURLValue);
        propertiesComponent.setValue(BetterDocsAction.SIZE, sizeValue);
        propertiesComponent.setValue(BetterDocsAction.DISTANCE, distanceValue);
    }

    @Override
    public void reset() {
        PropertiesComponent propertiesComponent = PropertiesComponent.getInstance();
        esURLText.setText(propertiesComponent.getValue(BetterDocsAction.ES_URL, BetterDocsAction.ES_URL_DEFAULT));
        sizeText.setText(propertiesComponent.getValue(BetterDocsAction.SIZE, String.valueOf(BetterDocsAction.SIZE_DEFAULT_VALUE)));
        distanceText.setText(propertiesComponent.getValue(BetterDocsAction.DISTANCE, String.valueOf(BetterDocsAction.DISTANCE_DEFAULT_VALUE)));
    }

    @Override
    public void disposeUIResources() {

    }
}
