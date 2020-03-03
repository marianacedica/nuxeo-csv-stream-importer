package org.nuxeo.ecm.platform.csv.importer.automation;

import static org.nuxeo.importer.stream.automation.BlobConsumers.DEFAULT_LOG_CONFIG;

import java.io.File;
import java.util.concurrent.ExecutionException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.automation.OperationContext;
import org.nuxeo.ecm.automation.OperationException;
import org.nuxeo.ecm.automation.core.Constants;
import org.nuxeo.ecm.automation.core.annotations.Context;
import org.nuxeo.ecm.automation.core.annotations.Operation;
import org.nuxeo.ecm.automation.core.annotations.OperationMethod;
import org.nuxeo.ecm.automation.core.annotations.Param;
import org.nuxeo.ecm.core.api.NuxeoException;
import org.nuxeo.ecm.core.api.NuxeoPrincipal;
import org.nuxeo.ecm.platform.csv.importer.producer.CSVDocumentMessageProducerFactory;
import org.nuxeo.importer.stream.automation.RandomBlobProducers;
import org.nuxeo.importer.stream.message.DocumentMessage;
import org.nuxeo.lib.stream.log.LogManager;
import org.nuxeo.lib.stream.pattern.producer.ProducerPool;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.stream.StreamService;

@Operation(id = CSVDocumentProducer.ID, category = Constants.CAT_SERVICES, label = "Reads a CSV File and produces docs messages", description = "")
public class CSVDocumentProducer {
    private static final Log log = LogFactory.getLog(RandomBlobProducers.class);

    public static final String ID = "StreamImporter.CSVDocumentProducer";

    public static final String DEFAULT_BLOB_LOG_NAME = "import-blob";

    @Context
    protected OperationContext ctx;

    @Param(name = "logName", required = false)
    protected String logName;

    @Param(name = "logSize", required = false)
    protected Integer logSize;

    @Param(name = "logConfig", required = false)
    protected String logConfig;

    @Param(name = "csvFilePath")
    protected String csvFilePath;

    @Param(name = "nbThreads", required = false)
    protected Integer nbThreads = 1;

    @OperationMethod
    public void run() throws OperationException {
        checkAccess(ctx);
        StreamService service = Framework.getService(StreamService.class);
        LogManager manager = service.getLogManager(getLogConfig());
        manager.createIfNotExists(getLogName(), getLogSize());

        File csvFile = new File(csvFilePath);
        if (!csvFile.exists()) {
            throw new NuxeoException("CSV file does not exist");
        }
        try {
            manager.createIfNotExists(getLogName(), getLogSize());
            // no point in having multiple producers per file since its not more performat to read one file with
            // multiple
            // threads
            // will change this here if we pass a folder path containing multiple files and we can have a pool of
            // producers/
            // one per file

            try (ProducerPool<DocumentMessage> producers = new ProducerPool<DocumentMessage>(getLogName(), manager,
                    new CSVDocumentMessageProducerFactory(csvFile), nbThreads.shortValue())) {
                producers.start().get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Operation interrupted");
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            log.error("Operation fails", e);
            throw new OperationException(e);
        }
    }

    protected String getLogConfig() {
        if (logConfig != null) {
            return logConfig;
        }
        return DEFAULT_LOG_CONFIG;
    }

    protected String getLogName() {
        if (logName != null) {
            return logName;
        }
        return DEFAULT_BLOB_LOG_NAME;
    }

    protected int getLogSize() {
        if (logSize != null && logSize > 0) {
            return logSize;
        }
        // return nbThreads;
        return 1;
    }

    protected static void checkAccess(OperationContext context) {
        NuxeoPrincipal principal = context.getPrincipal();
        if (principal == null || !principal.isAdministrator()) {
            throw new RuntimeException("Unauthorized access: " + principal);
        }
    }
}