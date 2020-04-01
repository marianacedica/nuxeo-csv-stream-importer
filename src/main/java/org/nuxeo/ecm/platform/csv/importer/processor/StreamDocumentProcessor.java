package org.nuxeo.ecm.platform.csv.importer.processor;

import org.nuxeo.ecm.platform.csv.importer.processor.computation.DocumentWriterComputation;
import org.nuxeo.lib.stream.computation.Topology;
import org.nuxeo.runtime.stream.StreamProcessorTopology;

import java.util.Collections;
import java.util.Map;

import static org.nuxeo.ecm.platform.csv.importer.ImporterConstants.STREAM_NAME;
import static org.nuxeo.lib.stream.computation.AbstractComputation.INPUT_1;

public class StreamDocumentProcessor implements StreamProcessorTopology {

    @Override
    public Topology getTopology(Map<String, String> map) {
        return Topology.builder()
                .addComputation(
                        DocumentWriterComputation::new,
                        Collections.singletonList(INPUT_1 + ":" + STREAM_NAME))
                .build();
    }
}
