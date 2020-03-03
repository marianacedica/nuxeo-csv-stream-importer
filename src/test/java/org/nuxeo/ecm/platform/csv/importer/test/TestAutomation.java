/*
 * (C) Copyright 2010 Nuxeo SA (http://nuxeo.com/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Benoit Delbosc
 */
package org.nuxeo.ecm.platform.csv.importer.test;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.automation.AutomationService;
import org.nuxeo.ecm.automation.OperationContext;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.ecm.platform.csv.importer.automation.CSVDocumentConsumers;
import org.nuxeo.ecm.platform.csv.importer.automation.CSVDocumentProducer;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.transaction.TransactionHelper;

@RunWith(FeaturesRunner.class)
@Features(CoreFeature.class)
@Deploy("org.nuxeo.runtime.stream")
@Deploy("org.nuxeo.importer.stream")
@Deploy("org.nuxeo.ecm.automation.core")
@Deploy("org.nuxeo.importer.stream.csv")
@Deploy("org.nuxeo.importer.stream.csv:test-stream-contrib.xml")
@Deploy("org.nuxeo.ecm.core.io")
public abstract class TestAutomation {

    @Inject
    CoreSession session;

    @Inject
    AutomationService automationService;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    public abstract void addExtraParams(Map<String, Object> params);

    @Test
    public void testCSVImport() throws Exception {

        // 1 . produce messages
        OperationContext ctx = new OperationContext(session);
        Map<String, Object> params = new HashMap<>();
        params.put("csvFilePath", getFilepath("sample-file.csv"));
        params.put("logName", "csv-import");
        addExtraParams(params);
        automationService.run(ctx, CSVDocumentProducer.ID, params);

        // 2. import document into the repository
        params.clear();
        params.put("rootFolder", "/");
        params.put("useBulkMode", true);
        params.put("blockDefaultSyncListeners", true);
        params.put("blockPostCommitListeners", true);
        params.put("blockAsyncListeners", true);
        params.put("blockIndexing", true);
        params.put("logName", "csv-import");
        addExtraParams(params);
        automationService.run(ctx, CSVDocumentConsumers.ID, params);

        // WorkManager service = Framework.getService(WorkManager.class);
        // assertTrue(service.awaitCompletion(10, TimeUnit.SECONDS));

        // start a new transaction to prevent db isolation to hide our new documents
        TransactionHelper.commitOrRollbackTransaction();
        TransactionHelper.startTransaction();

        int createdDocuments = session.query("SELECT * FROM Document WHERE ecm:primaryType IN ('File')").size();
        assertEquals(5, createdDocuments);

    }

    protected String getFilepath(String filename) {
        return this.getClass().getClassLoader().getResource(filename).getPath();
    }

}
