package org.nuxeo.ecm.platform.csv.importer.producer;

import java.io.File;
import java.io.FileNotFoundException;

import org.nuxeo.ecm.core.api.NuxeoException;
import org.nuxeo.importer.stream.message.DocumentMessage;
import org.nuxeo.lib.stream.pattern.producer.ProducerFactory;
import org.nuxeo.lib.stream.pattern.producer.ProducerIterator;

public class CSVDocumentMessageProducerFactory implements ProducerFactory<DocumentMessage> {

    protected final File csvFile;

    public CSVDocumentMessageProducerFactory(File csvFile) {
        this.csvFile = csvFile;
    }

    @Override
    public ProducerIterator<DocumentMessage> createProducer(int producerId) {
        try {
            return new CSVDocumentMessageProducer(producerId, csvFile);
        } catch (FileNotFoundException e) {
            throw new NuxeoException(e);
        }
    }
}
