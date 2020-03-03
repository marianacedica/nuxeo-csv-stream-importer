/*
 * (C) Copyright 2016 Nuxeo SA (http://nuxeo.com/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     bdelbosc
 */
package org.nuxeo.ecm.platform.csv.importer.consumer;

import java.io.Serializable;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.model.PropertyNotFoundException;
import org.nuxeo.importer.stream.consumer.DocumentMessageConsumer;
import org.nuxeo.importer.stream.message.DocumentMessage;

public class CustomDocumentMessageConsumer extends DocumentMessageConsumer {
    private static final Log log = LogFactory.getLog(CustomDocumentMessageConsumer.class);

    public CustomDocumentMessageConsumer(String consumerId, String repositoryName, String rootPath) {
        super(consumerId, repositoryName, rootPath);

    }

    @Override
    public void accept(DocumentMessage message) {
        DocumentModel doc = session.createDocumentModel(rootPath + message.getParentPath(), message.getName(),
                message.getType());
        doc.putContextData(CoreSession.SKIP_DESTINATION_CHECK_ON_CREATE, true);
        Blob blob = getBlob(message);
        if (blob != null) {
            // doc.setProperty("file", "filename", blob.getFilename());
            doc.setProperty("file", "content", blob);
        }
        Map<String, Serializable> props = message.getProperties();
        if (props != null && !props.isEmpty()) {
            setDocumentProperties(doc, props);
        }
        doc = session.createDocument(doc);
    }

    @Override
    protected void setDocumentProperties(DocumentModel doc, Map<String, Serializable> properties) {
        for (Map.Entry<String, Serializable> entry : properties.entrySet()) {
            try {
                if ("uid:major_version".equals(entry.getKey())) {

                } else if ("uid:minor_version".equals(entry.getKey())) {

                    // how to handle these?
                } else {
                    doc.setPropertyValue(entry.getKey(), entry.getValue());
                }
            } catch (PropertyNotFoundException e) {
                String message = String.format("Property '%s' not found on document type: %s. Skipping it.",
                        entry.getKey(), doc.getType());
                log.error(message, e);
            }
        }
    }

}
