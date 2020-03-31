package org.nuxeo.ecm.platform.csv.importer.processor.computation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.CloseableCoreSession;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.model.PropertyNotFoundException;
import org.nuxeo.ecm.core.blob.BlobInfo;
import org.nuxeo.ecm.core.blob.SimpleManagedBlob;
import org.nuxeo.importer.stream.message.DocumentMessage;

import java.io.Serializable;
import java.util.Map;

public final class DocumentCreation {

    private static final Log log = LogFactory.getLog(DocumentCreation.class);

    public static void createDocument(CoreSession session, DocumentMessage dcm) {
        log.debug("Create a document model for: " + dcm.getName() + " in '" + dcm.getParentPath() + "' with the type: " + dcm.getType());
        DocumentModel docToCreate = session.createDocumentModel(dcm.getParentPath(), dcm.getName(),
                dcm.getType());
        docToCreate.putContextData(CoreSession.SKIP_DESTINATION_CHECK_ON_CREATE, true);
        Blob blob = craftBlob(dcm);
        if (blob != null) {
            log.debug("Attaching a blob for document named: " + dcm.getName() + " in '" + dcm.getParentPath() + "' with the type: " + dcm.getType());
            docToCreate.setProperty("file", "content", blob);
        }
        Map<String, Serializable> props = dcm.getProperties();
        if (props != null && !props.isEmpty()) {
            log.debug("Adding additional properties for document named: " + dcm.getName() + " in '" + dcm.getParentPath() + "' with the type: " + dcm.getType());
            addingPropertyValues(docToCreate, props);
        }
        log.debug("Creating a document named: " + dcm.getName() + " in '" + dcm.getParentPath() + "' with the type: " + dcm.getType());
        session.createDocument(docToCreate);
    }

    protected static Blob craftBlob(DocumentMessage message) {
        Blob blob = null;
        if (message.getBlob() != null) {
            blob = message.getBlob();
        } else if (message.getBlobInfo() != null) {
            BlobInfo blobInfo = message.getBlobInfo();
            blob = new SimpleManagedBlob(blobInfo);
        }
        return blob;
    }

    protected static void addingPropertyValues(DocumentModel doc, Map<String, Serializable> properties) throws PropertyNotFoundException {
        for (Map.Entry<String, Serializable> entry : properties.entrySet()) {
            try {
                doc.setPropertyValue(entry.getKey(), entry.getValue());
            } catch (PropertyNotFoundException e) {
                String message = String.format("Property '%s' not found on document type: %s, with id: %s.",
                        entry.getKey(), doc.getType(), doc.getId());
                log.error(message, e);
                throw e;
            }
        }
    }
}
