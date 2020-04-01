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
import org.nuxeo.ecm.platform.csv.importer.transformer.LineToRecord;
import org.nuxeo.ecm.platform.csv.importer.transformer.bean.LineBean;
import org.nuxeo.lib.stream.computation.Record;
import org.nuxeo.lib.stream.log.LogAppender;
import org.nuxeo.lib.stream.log.LogManager;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.stream.StreamService;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.function.Function;

import static org.nuxeo.ecm.platform.csv.importer.ImporterConstants.*;

@Operation(id = CSVDocumentProducer.ID, category = Constants.CAT_SERVICES, label = "Reads a CSV File and produces docs messages", description = "")
public class CSVDocumentProducer {
    private static final Log log = LogFactory.getLog(CSVDocumentProducer.class);

    public static final String ID = "StreamImporter.CSVDocumentProducer";

    // First partition is ZERO
    private static final int FIRST_PARTITION = 0;

    private static final String DEFAULT_SEPARATOR = ",";
    public static final String CSV_FILE_PATH = "csvFilePath";
    public static final String FUID = "fuid";
    public static final String FORCE_SINGLE_CONSUMER_PRODUCER = "forceSingleConsumerProducer";

    private static Long nbPartitions;

    @Context
    protected OperationContext ctx;

    @Param(name = FUID, required = false)
    protected String fuid;

    @Param(name = CSV_FILE_PATH, required = false)
    protected String csvFilePath;

    @Param(name = FORCE_SINGLE_CONSUMER_PRODUCER, required = false)
    protected Boolean forceSingleConsumerProducer = Boolean.FALSE;

    @OperationMethod
    public void run() throws OperationException, FileNotFoundException {

        checkAccess(ctx);

        StreamService service = Framework.getService(StreamService.class);
        LogManager manager = service.getLogManager(CSV_STREAM_IMPORT_CONFIG);
        LogAppender<Record> appender = manager.getAppender(STREAM_NAME);

        if (csvFilePath == null && fuid == null) {
            throw new NuxeoException("Operation not initialized properly, specify either the 'fuid' or 'csvFilePath' parameter.");
        }

        if (fuid != null) {
            if (craftCSVFilePath() == null) {
                throw new NuxeoException("The nuxeo.conf property: " + NUXEO_STREAM_IMPORT_PATH_ROOT + " is not defined.");
            }
            csvFilePath = craftCSVFilePath() + "/" + fuid + "/" + fuid + ".csv";
        }

        log.info("CSV File Path: " + csvFilePath);
        File csvFile = new File(csvFilePath);
        if (!csvFile.exists()) {
            throw new NuxeoException("CSV file does not exist");
        }

        // Extract header
        List<String> columnsHeaders = new ArrayList<>();
        Scanner scanner = new Scanner(csvFile);
        if (scanner.hasNext()) {
            // header line
            columnsHeaders = Arrays.asList(scanner.nextLine().split(DEFAULT_SEPARATOR));
            log.debug("CSV Header: " + columnsHeaders);
        }

        // Round robin repartition
        Long roundRobinCounter = 0L;
        while (scanner.hasNext()) {

            final Long currentPartition = roundRobinCounter % getLogSize();

            List<String> columns = Arrays.asList(scanner.nextLine().split(DEFAULT_SEPARATOR));

            log.debug("CSV Line [" + roundRobinCounter + "] columns values:" + columns);
            final LineBean lineBean = new LineBean(columnsHeaders, columns, roundRobinCounter, csvFilePath);

            // Force only one partition to manage folder creation first. Structure needs to be sorted by path
            if (forceSingleConsumerProducer) {
                log.debug("Forced single partition / Allocate message [" + lineBean + "] to partition [" + FIRST_PARTITION + "] of [" + getLogSize() + "]");
                appender.append(FIRST_PARTITION, transformToRecord().apply(lineBean));
            } else {
                log.debug("Allocate message [" + lineBean + "] to partition [" + currentPartition + "] of [" + getLogSize() + "]");
                appender.append(currentPartition.intValue(), transformToRecord().apply(lineBean));
            }
            roundRobinCounter++;
        }
    }

    protected Long getLogSize() {
        if (nbPartitions == null) {
            nbPartitions = Long.parseLong(Framework.getProperty(NUXEO_STREAM_IMPORT_DEFAULT_PARTITIONS, PARTITION_NUMBER_DEF_VALUE));
        }
        return nbPartitions;
    }


    protected String craftCSVFilePath() {
        return Framework.getProperty(NUXEO_STREAM_IMPORT_PATH_ROOT);
    }


    protected static void checkAccess(OperationContext context) {
        NuxeoPrincipal principal = context.getPrincipal();
        if (principal == null || !principal.isAdministrator()) {
            throw new RuntimeException("Unauthorized access: " + principal);
        }
    }

    Function<LineBean, Record> transformToRecord() {
        return Framework.getService(LineToRecord.class).transform();
    }
}