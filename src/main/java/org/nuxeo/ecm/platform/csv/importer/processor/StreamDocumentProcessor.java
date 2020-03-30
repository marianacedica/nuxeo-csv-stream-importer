package org.nuxeo.ecm.platform.csv.importer.processor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.lib.stream.computation.AbstractBatchComputation;
import org.nuxeo.lib.stream.computation.ComputationContext;
import org.nuxeo.lib.stream.computation.Record;
import org.nuxeo.lib.stream.computation.Topology;
import org.nuxeo.runtime.stream.StreamProcessorTopology;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.nuxeo.ecm.platform.csv.importer.ImporterConstants.COMPUTATION_NAME;
import static org.nuxeo.ecm.platform.csv.importer.ImporterConstants.STREAM_NAME;
import static org.nuxeo.lib.stream.computation.AbstractComputation.INPUT_1;

public class StreamDocumentProcessor implements StreamProcessorTopology {

    private static final Log log = LogFactory.getLog(StreamDocumentProcessor.class);

    @Override
    public Topology getTopology(Map<String, String> map) {
        return Topology.builder()
                .addComputation(
                        DocumentWriterComputation::new,
                        Collections.singletonList(INPUT_1 + ":" + STREAM_NAME))
                .build();
    }

    public static class DocumentWriterComputation extends AbstractBatchComputation {

        public DocumentWriterComputation() {
            super(COMPUTATION_NAME, 1, 0);
        }

        @Override
        protected void batchProcess(ComputationContext context, String inputStreamName, List<Record> records) {
            records.forEach(x -> {
                String value = new String(x.getData(), Charset.defaultCharset());
                log.error(x.getKey() + " " + value);
            });
        }

        @Override
        public void batchFailure(ComputationContext computationContext, String s, List<Record> list) {
            list.forEach(x -> {
                String value = new String(x.getData(), Charset.defaultCharset());
                log.error(x.getKey() + " " + value);
            });
        }
    }
}
