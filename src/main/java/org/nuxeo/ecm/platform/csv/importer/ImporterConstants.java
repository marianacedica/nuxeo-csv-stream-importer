package org.nuxeo.ecm.platform.csv.importer;

public class ImporterConstants {

    public static final String NUXEO_STREAM_IMPORT_BATCH_THRESHOLD_MS = "nuxeo.stream.import.batch.threshold.ms";
    public static final String NUXEO_STREAM_IMPORT_DEFAULT_PARTITIONS = "nuxeo.stream.import.default.partitions";
    public static final String NUXEO_STREAM_IMPORT_BATCH_PARTITIONS = "nuxeo.stream.import.batch.partitions";
    public static final String NUXEO_STREAM_IMPORT_BATCH_CONCURRENCY = "nuxeo.stream.import.batch.concurrency";

    public static final String NUXEO_STREAM_IMPORT_PATH_ROOT = "nuxeo.stream.import.path.root";

    public static final String COMPUTATION_NAME = "CSVStreamImportWriter";

    public static final String CSV_STREAM_IMPORT_CONFIG = "csv-stream-import-config";

    public static final String PARTITION_NUMBER_DEF_VALUE = "3";

    public static final String STREAM_NAME = "csv-stream-import";

    private ImporterConstants() throws IllegalAccessException {
        throw new IllegalAccessException("Cannot instantiate class: " + ImporterConstants.class.getName());
    }
}
