package org.nuxeo.ecm.platform.csv.importer.automation;

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
import org.nuxeo.lib.stream.computation.Record;
import org.nuxeo.lib.stream.log.LogAppender;
import org.nuxeo.lib.stream.log.LogManager;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.stream.StreamService;

import java.io.File;
import java.nio.charset.Charset;
import java.util.stream.IntStream;

import static org.nuxeo.ecm.platform.csv.importer.ImporterConstants.*;

@Operation(id = CSVDocumentProducer.ID, category = Constants.CAT_SERVICES, label = "Reads a CSV File and produces docs messages", description = "")
public class CSVDocumentProducer {
    private static final Log log = LogFactory.getLog(CSVDocumentProducer.class);

    public static final String ID = "StreamImporter.CSVDocumentProducer";

    // First partition is ZERO
    private static final int FIRST_PARTITION = 0;

    @Context
    protected OperationContext ctx;


    @Param(name = "csvFilePath")
    protected String csvFilePath;

    @OperationMethod
    public void run() throws OperationException {
        checkAccess(ctx);
        StreamService service = Framework.getService(StreamService.class);
        LogManager manager = service.getLogManager(CSV_STREAM_IMPORT_CONFIG);
        log.error("csvFilePath: " + csvFilePath);
        File csvFile = new File(csvFilePath);
        if (!csvFile.exists()) {
            throw new NuxeoException("CSV file does not exist");
        }

        byte[] data = "value007".getBytes(Charset.defaultCharset());
        LogAppender<Record> appender = manager.getAppender(STREAM_NAME);

        log.error("Number of partitions: " + getLogSize());
        //TODO: Should loop over the csv lines and create a Record that contain each document + round robin to spread the load
        IntStream.range(1, 20).forEach(x -> {
                    appender.append(FIRST_PARTITION, Record.of(csvFilePath + x, data));
                }
        );

    }

    protected int getLogSize() {
        return Integer.parseInt(Framework.getProperty(KEY_PARTITION_NUMBER, PARTITION_NUMBER_DEF_VALUE));
    }

    protected static void checkAccess(OperationContext context) {
        NuxeoPrincipal principal = context.getPrincipal();
        if (principal == null || !principal.isAdministrator()) {
            throw new RuntimeException("Unauthorized access: " + principal);
        }
    }
}