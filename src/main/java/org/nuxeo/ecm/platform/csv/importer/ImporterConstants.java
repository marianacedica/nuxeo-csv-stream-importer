package org.nuxeo.ecm.platform.csv.importer;

public class ImporterConstants {

    public static final String COMPUTATION_NAME = "CSVStreamImportWriter";

    public static final String CSV_STREAM_IMPORT_CONFIG = "csv-stream-import-config";

    public static final String KEY_PARTITION_NUMBER = "nuxeo.stream.import.default.partitions";
    public static final String PARTITION_NUMBER_DEF_VALUE = "1";

    public static final String STREAM_NAME = "csv-stream-import";

    private ImporterConstants() throws IllegalAccessException {
        throw new IllegalAccessException("Cannot instantiate class: " + ImporterConstants.class.getName());
    }
}
