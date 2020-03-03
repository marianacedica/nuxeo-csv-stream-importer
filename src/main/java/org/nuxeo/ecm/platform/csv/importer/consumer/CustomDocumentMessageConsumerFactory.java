package org.nuxeo.ecm.platform.csv.importer.consumer;

import org.nuxeo.importer.stream.consumer.DocumentMessageConsumerFactory;
import org.nuxeo.importer.stream.message.DocumentMessage;
import org.nuxeo.lib.stream.pattern.consumer.Consumer;

public class CustomDocumentMessageConsumerFactory extends DocumentMessageConsumerFactory {

    public CustomDocumentMessageConsumerFactory(String repositoryName, String rootPath) {
        super(repositoryName, rootPath);
    }

    @Override
    public Consumer<DocumentMessage> createConsumer(String consumerId) {
        return new CustomDocumentMessageConsumer(consumerId, repositoryName, rootPath);
    }

}
