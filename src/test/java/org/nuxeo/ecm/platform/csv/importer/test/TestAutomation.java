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

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.automation.AutomationService;
import org.nuxeo.ecm.automation.OperationContext;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.NuxeoException;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.ecm.platform.csv.importer.automation.CSVDocumentProducer;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.transaction.TransactionHelper;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.nuxeo.ecm.platform.csv.importer.ImporterConstants.*;
import static org.nuxeo.ecm.platform.csv.importer.automation.CSVDocumentProducer.*;

@RunWith(FeaturesRunner.class)
@Features(CoreFeature.class)
@Deploy({
        "org.nuxeo.runtime.stream",
        "org.nuxeo.importer.stream",
        "org.nuxeo.ecm.automation.core",
        "org.nuxeo.importer.stream.csv",
        "org.nuxeo.importer.stream.csv:test-stream-contrib.xml",
        "org.nuxeo.ecm.core.io"
})
public abstract class TestAutomation {

    private static final String FOLDER = "Folder";
    private static final String SAMPLE_FILE_CSV = "sample-file/sample-file.csv";
    private static final String SAMPLE_FILE_CSV_FUID = "sample-file";
    private static final String SAMPLE_FILE_TYPE_ONLY_CSV = "sample-file-type-only.csv";

    private static List<FolderDescription> INIT_FOLDERS = new ArrayList<FolderDescription>() {{
        add(new FolderDescription("AP_Correspondence", "/"));
        add(new FolderDescription("W-9", "/AP_Correspondence"));
        add(new FolderDescription("Direct Deposit", "/AP_Correspondence"));
        add(new FolderDescription("Unit Test", "/AP_Correspondence"));
        add(new FolderDescription("W-9", "/AP_Correspondence"));
        add(new FolderDescription("Butter", "/AP_Correspondence/W-9"));
        add(new FolderDescription("Is", "/AP_Correspondence/W-9/Butter"));
        add(new FolderDescription("Great", "/AP_Correspondence/W-9/Butter/Is"));
    }};

    @Inject
    CoreSession session;

    @Inject
    AutomationService automationService;

    @Inject
    protected CoreFeature coreFeature;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    public abstract void addExtraParams(Map<String, Object> params);

    @Before
    public void init() {

        if (Framework.getProperty(NUXEO_STREAM_IMPORT_PATH_ROOT) != null) {
            Framework.getProperties().remove(NUXEO_STREAM_IMPORT_PATH_ROOT);
        }

        Framework.getProperties().put(NUXEO_STREAM_IMPORT_BATCH_THRESHOLD_MS, "50ms");
        Framework.getProperties().put(NUXEO_STREAM_IMPORT_DEFAULT_PARTITIONS, "3");
        Framework.getProperties().put(NUXEO_STREAM_IMPORT_BATCH_PARTITIONS, "12");
        Framework.getProperties().put(NUXEO_STREAM_IMPORT_BATCH_CONCURRENCY, "3");
    }

    void initFolders() {
        INIT_FOLDERS.forEach(folder -> {
            DocumentModel dcm = session.createDocumentModel(folder.path, folder.name, FOLDER);
            session.createDocument(dcm);
        });
        session.save();
    }

    @Test
    public void testCSVImportOnlyFilesAndMultiThreading() throws Exception {
        // Init Structure
        initFolders();

        // 1 . produce messages
        OperationContext ctx = new OperationContext(session);
        Map<String, Object> params = new HashMap<String, Object>() {{
            put(CSV_FILE_PATH, getFilepath(SAMPLE_FILE_TYPE_ONLY_CSV));
            put(FORCE_SINGLE_CONSUMER_PRODUCER, false);
        }};
        addExtraParams(params);
        automationService.run(ctx, CSVDocumentProducer.ID, params);

        // 2. import document into the repository
        coreFeature.waitForAsyncCompletion();
        Thread.sleep(10000);
        // start a new transaction to prevent db isolation to hide our new documents
        TransactionHelper.commitOrRollbackTransaction();
        TransactionHelper.startTransaction();


        int createdDocuments = session.query("SELECT * FROM Document WHERE ecm:primaryType IN ('File')").size();
        assertEquals(5, createdDocuments);

    }

    @Test
    public void testCSVImportFilesAndFolderOneThread() throws Exception {

        // 1 . produce messages
        OperationContext ctx = new OperationContext(session);
        Map<String, Object> params = new HashMap<String, Object>() {{
            put(CSV_FILE_PATH, getFilepath(SAMPLE_FILE_CSV));
            put(FORCE_SINGLE_CONSUMER_PRODUCER, true);
        }};
        addExtraParams(params);
        automationService.run(ctx, CSVDocumentProducer.ID, params);

        // 2. import document into the repository

        coreFeature.waitForAsyncCompletion();
        Thread.sleep(10000);
        // start a new transaction to prevent db isolation to hide our new documents
        TransactionHelper.commitOrRollbackTransaction();
        TransactionHelper.startTransaction();


        int createdDocuments = session.query("SELECT * FROM Document WHERE ecm:primaryType IN ('File')").size();
        assertEquals(5, createdDocuments);

    }

    @Test(expected = NuxeoException.class)
    public void testCSVImportNoParameters() throws Exception {
        // GIVEN
        OperationContext ctx = new OperationContext(session);
        Map<String, Object> params = new HashMap<>();
        addExtraParams(params);

        // WHEN
        try {
            automationService.run(ctx, CSVDocumentProducer.ID, params);
        } catch (NuxeoException ex) {
            Assertions.assertThat(ex.getMessage()).isEqualTo("Failed to invoke operation StreamImporter.CSVDocumentProducer, Operation not initialized properly, specify either the 'fuid' or 'csvFilePath' parameter.");
            throw ex;
        }
        fail("Should fail this none of the required parameter is set");

    }

    @Test
    public void testCSVImportFilesAndFolderOneThreadFUIDNotNullAndConfNotNull() throws Exception {

        // GIVEN
        String sampleFilePath = this.getClass().getClassLoader().getResource(SAMPLE_FILE_CSV).getPath();
        String rootPath = sampleFilePath.substring(0, sampleFilePath.indexOf(SAMPLE_FILE_CSV));
        Framework.getProperties().put(NUXEO_STREAM_IMPORT_PATH_ROOT, rootPath);

        // 1 . produce messages
        OperationContext ctx = new OperationContext(session);
        Map<String, Object> params = new HashMap<String, Object>() {{
            put(FUID, SAMPLE_FILE_CSV_FUID);
            put(FORCE_SINGLE_CONSUMER_PRODUCER, true);
        }};
        addExtraParams(params);

        // WHEN
        automationService.run(ctx, CSVDocumentProducer.ID, params);

        // 2. import document into the repository
        coreFeature.waitForAsyncCompletion();
        Thread.sleep(10000);
        // start a new transaction to prevent db isolation to hide our new documents
        TransactionHelper.commitOrRollbackTransaction();
        TransactionHelper.startTransaction();

        int createdDocuments = session.query("SELECT * FROM Document WHERE ecm:primaryType IN ('File')").size();
        assertEquals(5, createdDocuments);
    }

    @Test(expected = NuxeoException.class)
    public void testCSVImportFilesAndFolderOneThreadRootPathNull() throws Exception {

        // 1 . produce messages
        OperationContext ctx = new OperationContext(session);
        Map<String, Object> params = new HashMap<String, Object>() {{
            put(FUID, SAMPLE_FILE_CSV_FUID);
            put(FORCE_SINGLE_CONSUMER_PRODUCER, true);
        }};
        addExtraParams(params);
        try {
            automationService.run(ctx, CSVDocumentProducer.ID, params);
        } catch (NuxeoException ex) {
            Assertions.assertThat(ex.getMessage()).isEqualTo("Failed to invoke operation StreamImporter.CSVDocumentProducer, The nuxeo.conf property: nuxeo.stream.import.path.root is not defined.");
            throw ex;
        }
        fail("Should not be able to start since " + NUXEO_STREAM_IMPORT_PATH_ROOT + " is not set");
    }

    protected String getFilepath(String filename) {
        return this.getClass().getClassLoader().getResource(filename).getPath();
    }

    static class FolderDescription {
        String name;
        String path;

        public FolderDescription(String name, String path) {
            this.name = name;
            this.path = path;
        }
    }

}
