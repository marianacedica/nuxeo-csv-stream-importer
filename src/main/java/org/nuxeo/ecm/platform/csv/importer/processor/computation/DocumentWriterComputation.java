package org.nuxeo.ecm.platform.csv.importer.processor.computation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.core.api.CloseableCoreSession;
import org.nuxeo.ecm.platform.csv.importer.transformer.EncodeDecode;
import org.nuxeo.importer.stream.message.DocumentMessage;
import org.nuxeo.lib.stream.computation.AbstractBatchComputation;
import org.nuxeo.lib.stream.computation.ComputationContext;
import org.nuxeo.lib.stream.computation.Record;
import org.nuxeo.runtime.transaction.TransactionHelper;

import java.util.List;

import static org.nuxeo.ecm.core.api.CoreInstance.openCoreSessionSystem;
import static org.nuxeo.ecm.platform.csv.importer.ImporterConstants.COMPUTATION_NAME;

public class DocumentWriterComputation extends AbstractBatchComputation {

    private static final Log log = LogFactory.getLog(DocumentWriterComputation.class);

    public DocumentWriterComputation() {
        super(COMPUTATION_NAME, 1, 0);
    }

    @Override
    protected void batchProcess(ComputationContext context, String inputStreamName, List<Record> records) {

        if (TransactionHelper.isNoTransaction()) {
            TransactionHelper.startTransaction();
        }
        try (final CloseableCoreSession session = openCoreSessionSystem(null)) {
            records.forEach(record -> {
                DocumentMessage dcm = EncodeDecode.decode.apply(record.getData());
                log.info("Will create a document named: " + dcm.getName() + " in '" + dcm.getParentPath() + "' with the type: " + dcm.getType());
                DocumentCreation.createDocument(session, dcm);
            });
        } finally {
            TransactionHelper.commitOrRollbackTransaction();
        }
    }

    @Override
    public void batchFailure(ComputationContext computationContext, String s, List<Record> list) {
        list.forEach(record -> {
            DocumentMessage dcm = EncodeDecode.decode.apply(record.getData());
            log.error("An error happened will trying to create a document named: " + dcm.getName() + " in '" + dcm.getParentPath() + "' with the type: " + dcm.getType());
        });
    }
}
