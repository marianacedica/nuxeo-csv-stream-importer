package org.nuxeo.ecm.platform.csv.importer.producer;

import org.nuxeo.ecm.core.api.NuxeoException;
import org.nuxeo.ecm.platform.csv.importer.message.MessageRecord;
import org.nuxeo.lib.stream.pattern.producer.ProducerFactory;
import org.nuxeo.lib.stream.pattern.producer.ProducerIterator;

import java.io.File;
import java.io.FileNotFoundException;

public class CSVDocumentMessageProducerFactory implements ProducerFactory<MessageRecord> {

    protected final File csvFile;

    public CSVDocumentMessageProducerFactory(File csvFile) {
        this.csvFile = csvFile;
    }

    @Override
    public ProducerIterator<MessageRecord> createProducer(int producerId) {
        try {
            return new CSVDocumentMessageProducer(producerId, csvFile);
        } catch (FileNotFoundException e) {
            throw new NuxeoException(e);
        }
    }
}
