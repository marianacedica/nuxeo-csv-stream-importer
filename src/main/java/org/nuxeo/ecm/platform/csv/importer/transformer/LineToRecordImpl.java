package org.nuxeo.ecm.platform.csv.importer.transformer;

import org.nuxeo.ecm.platform.csv.importer.transformer.bean.LineBean;
import org.nuxeo.importer.stream.message.DocumentMessage;
import org.nuxeo.lib.stream.computation.Record;
import org.nuxeo.runtime.model.DefaultComponent;

import java.io.Serializable;
import java.util.HashMap;
import java.util.function.Function;

public class LineToRecordImpl extends DefaultComponent implements LineToRecord {

    protected static Function<DocumentMessage, Record> DOCMESSAGE_TO_RECORD = (documentMessage) -> Record.of(documentMessage.getId(), EncodeDecode.encode.apply(documentMessage));

    protected static Function<LineBean, DocumentMessage> LINE_TO_DOCMESSAGE = (linebean) -> {
        if (linebean.hasValidData()) {
            // CSV format
            // title,type,description,path,otherProperties
            // NOTE: path must point to an existing parent path
            // The importer doesn't yet handle creating parent folder during import
            String docId = linebean.getColumns().get(0);
            String type = linebean.getColumns().get(1);
            String parentPath = "/";
            String title = linebean.getColumns().get(2);
            // if dc:title contains the path

            HashMap<String, Serializable> properties = new HashMap<>();
            // folders only have dc:title
            if (linebean.getColumns().size() > 3) {
                parentPath = linebean.getColumns().get(4);
            }
            if (linebean.getColumns().size() > 5) {
                properties = parseProperties(linebean);
            } else {
                properties.put("dc:title", linebean.getColumns().get(2));
            }
            // Path must be RELATIVE pat;h in nuxeo, and folder MUST EXIST before
            // If importing from default-domain:
            // workspaces/administrator/myFolder
            // If importing from /:
            // /default-domain/workspaces/administrator/myFolder

            return DocumentMessage.builder(type, parentPath, title).setProperties(properties).build();
        }
        return null;
    };

    protected static HashMap<String, Serializable> parseProperties(LineBean lineBean) {
        HashMap<String, Serializable> ret = new HashMap<>();
        for (int i = 0; i < lineBean.getHeaders().size(); i++) {
            // change me, only search for properties
            if (lineBean.getHeaders().get(i).contains(":")) {
                ret.put(lineBean.getHeaders().get(i), lineBean.getColumns().get(i));
            }
        }
        return ret;
    }

    public Function<LineBean, Record> transform() {
        return (linebean) -> LINE_TO_DOCMESSAGE.andThen(DOCMESSAGE_TO_RECORD).apply(linebean);
    }
}
