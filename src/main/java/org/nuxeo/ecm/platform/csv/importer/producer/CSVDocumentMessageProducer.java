package org.nuxeo.ecm.platform.csv.importer.producer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.core.io.registry.MarshallerHelper;
import org.nuxeo.ecm.core.io.registry.context.RenderingContext;
import org.nuxeo.ecm.platform.csv.importer.message.MessageRecord;
import org.nuxeo.importer.stream.message.DocumentMessage;
import org.nuxeo.lib.stream.pattern.producer.AbstractProducer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;

public class CSVDocumentMessageProducer extends AbstractProducer<MessageRecord> {
    private static final Log log = LogFactory.getLog(CSVDocumentMessageProducer.class);

    private static final String DEFAULT_SEPARATOR = ",";

    protected Scanner scanner;

    protected Stream<String> lines;

    List<String> columnsHeaders = new ArrayList<>();

    public CSVDocumentMessageProducer(int producerId, File csvFile) throws FileNotFoundException {
        super(producerId);
        scanner = new Scanner(csvFile);
        if (scanner.hasNext()) {
            // header line
            columnsHeaders = Arrays.asList(scanner.nextLine().split(DEFAULT_SEPARATOR));
        }

    }

    @Override
    public int getPartition(MessageRecord message, int partitions) {
        // this has to be changed
        // You can use map key value where key is the parent path and value is the partition key
        // using message.getParentPath()
        // </path/folder1,key1>,</path/folder2,key2>
        // for now will use one partition
        return 0;

    }

    @Override
    public boolean hasNext() {
        return scanner.hasNext();
    }

    @Override
    public MessageRecord next() {
        String[] columns = scanner.nextLine().split(DEFAULT_SEPARATOR);
        DocumentMessage dc = createDocument(columns);
        // Create a Record
        return MessageRecord.of(dc.getId(), asJson(dc).getBytes(UTF_8));
    }

    protected HashMap<String, Serializable> parseProperties(String[] col) {
        HashMap<String, Serializable> ret = new HashMap<>();
        for (int i = 0; i < columnsHeaders.size(); i++) {
            // change me, only search for properties
            if (columnsHeaders.get(i).contains(":")) {
                ret.put(columnsHeaders.get(i), col[i]);
            }
        }
        return ret;
    }

    protected DocumentMessage createDocument(String[] col) {
        // CSV format
        // title,type,description,path,otherProperties
        // NOTE: path must point to an existing parent path
        // The importer doesn't yet handle creating parent folder during import
        String docId = col[0];
        String type = col[1];
        String parentPath = "/";
        String title = col[2];
        // if dc:title contains the path

        HashMap<String, Serializable> properties = new HashMap<>();
        // folders only have dc:title
        if (col.length > 3) {
            parentPath = col[4];
        }
        if (col.length > 5) {
            properties = parseProperties(col);
        } else {
            properties.put("dc:title", col[2]);
        }
        // Path must be RELATIVE pat;h in nuxeo, and folder MUST EXIST before
        // If importing from default-domain:
        // workspaces/administrator/myFolder
        // If importing from /:
        // /default-domain/workspaces/administrator/myFolder

        DocumentMessage.Builder builder = DocumentMessage.builder(type, parentPath, title).setProperties(properties);

        return builder.build();
    }


    protected String asJson(DocumentMessage entry) {
        if (entry == null) {
            return null;
        }
        RenderingContext ctx = RenderingContext.CtxBuilder.get();
        try {
            return MarshallerHelper.objectToJson(entry, ctx);
        } catch (IOException e) {
            log.warn("Unable to translate entry into json, entry:" + entry.getId() + ": " + e.getMessage(), e);
            return null;
        }
    }
}
