package org.nuxeo.ecm.platform.csv.importer.message;

import org.nuxeo.lib.stream.computation.Record;
import org.nuxeo.lib.stream.pattern.Message;

public class MessageRecord extends Record implements Message {

    public MessageRecord(String key, byte[] data) {
        super(key, data);
    }

    @Override
    public String getId() {
        return getKey();
    }

    public static MessageRecord of(String key, byte[] data) {
        return new MessageRecord(key, data);
    }
}
