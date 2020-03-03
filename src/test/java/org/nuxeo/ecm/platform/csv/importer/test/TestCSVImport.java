/*
 * (C) Copyright 2016 Nuxeo SA (http://nuxeo.com/) and others.
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
 *     bdelbosc
 */
package org.nuxeo.ecm.platform.csv.importer.test;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.api.repository.RepositoryManager;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.ecm.platform.csv.importer.consumer.CustomDocumentMessageConsumerFactory;
import org.nuxeo.ecm.platform.csv.importer.producer.CSVDocumentMessageProducerFactory;
import org.nuxeo.importer.stream.message.DocumentMessage;
import org.nuxeo.lib.stream.log.LogManager;
import org.nuxeo.lib.stream.pattern.consumer.BatchPolicy;
import org.nuxeo.lib.stream.pattern.consumer.ConsumerPolicy;
import org.nuxeo.lib.stream.pattern.consumer.ConsumerPool;
import org.nuxeo.lib.stream.pattern.consumer.ConsumerStatus;
import org.nuxeo.lib.stream.pattern.producer.ProducerPool;
import org.nuxeo.lib.stream.pattern.producer.ProducerStatus;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;

@RunWith(FeaturesRunner.class)
@Features(CoreFeature.class)
@Deploy("org.nuxeo.runtime.stream")
public abstract class TestCSVImport {
    protected static final Log log = LogFactory.getLog(TestCSVImport.class);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    public abstract LogManager getManager() throws Exception;

    Integer nbThreadsProducers = 1;

    Integer nbThreadsConsumers = 5;

    final short NB_PRODUCERS = 1;

    @Inject
    RepositoryManager repositoryManager;

    @Test
    public void fileCSVImporterImporter() throws Exception {

        try (LogManager manager = getManager()) {
            manager.createIfNotExists("csv-import", 1);

            try (ProducerPool<DocumentMessage> producers = new ProducerPool<DocumentMessage>("csv-import", manager,
                    new CSVDocumentMessageProducerFactory(getFile("sample-file.csv")),
                    nbThreadsProducers.shortValue())) {
                List<ProducerStatus> ret = producers.start().get();

                // assert nr of messages produced equals nr of documents in the csv file
                assertEquals(13, ret.stream().mapToLong(r -> r.nbProcessed).sum());

                try (LogManager managerBlobInfo = getManager()) {
                    managerBlobInfo.createIfNotExists("csv-import", 1);
                    ConsumerPool<DocumentMessage> consumers = new ConsumerPool<DocumentMessage>("csv-import", manager,
                            new CustomDocumentMessageConsumerFactory(repositoryManager.getDefaultRepositoryName(), "/"),
                            ConsumerPolicy.builder().batchPolicy(BatchPolicy.NO_BATCH).build());
                    List<ConsumerStatus> retConsumers = consumers.start().get();
                    // we only have one consumer and 13 messages as we need to create also the folders before
                    assertEquals(13, retConsumers.stream().mapToLong(r -> r.committed).sum());
                }
            }
        }
    }

    protected File getFile(String filename) {
        return new File(this.getClass().getClassLoader().getResource(filename).getPath());
    }

    protected String getBasePathList(String base) {
        return this.getClass().getClassLoader().getResource(base).getPath();
    }

}