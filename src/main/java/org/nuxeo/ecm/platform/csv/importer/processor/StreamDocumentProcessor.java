package org.nuxeo.ecm.platform.csv.importer.processor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.lib.stream.computation.AbstractBatchComputation;
import org.nuxeo.lib.stream.computation.ComputationContext;
import org.nuxeo.lib.stream.computation.Record;
import org.nuxeo.lib.stream.computation.Topology;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.stream.StreamProcessorTopology;
import org.nuxeo.runtime.transaction.TransactionHelper;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class StreamDocumentProcessor implements StreamProcessorTopology {

    private static final Log log = LogFactory.getLog(StreamDocumentProcessor.class);
    private static final String COMPUTATION_NAME = "ComputationStreamImporter";

    private static final String KEY_STREAM_NAME = "nuxeo.stream.import.log.config";
    private static final String STREAM_NAME_DEFAULT = "stream-import";

    @Override
    public Topology getTopology(Map<String, String> map) {
        return Topology.builder()
                .addComputation(
                        () -> new DocumentWriterComputation(COMPUTATION_NAME),
                        Collections.singletonList("i1:" + Framework.getProperty(KEY_STREAM_NAME, STREAM_NAME_DEFAULT)))
                .build();
    }

    public static class DocumentWriterComputation extends AbstractBatchComputation {

        public DocumentWriterComputation(String name) {
            super(name, 1, 0);
        }

        @Override
        protected void batchProcess(ComputationContext computationContext, String s, List<Record> list) {

        }

        @Override
        public void batchFailure(ComputationContext computationContext, String s, List<Record> list) {

        }
    }
}
