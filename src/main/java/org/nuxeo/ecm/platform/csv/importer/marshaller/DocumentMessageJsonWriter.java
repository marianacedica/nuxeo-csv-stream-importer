package org.nuxeo.ecm.platform.csv.importer.marshaller;

import com.fasterxml.jackson.core.JsonGenerator;
import org.nuxeo.ecm.core.io.marshallers.json.ExtensibleEntityJsonWriter;
import org.nuxeo.ecm.core.io.registry.reflect.Setup;
import org.nuxeo.ecm.platform.csv.importer.message.MessageRecord;

import java.io.IOException;

import static org.nuxeo.ecm.core.io.registry.reflect.Instantiations.SINGLETON;
import static org.nuxeo.ecm.core.io.registry.reflect.Priorities.REFERENCE;

@Setup(mode = SINGLETON, priority = REFERENCE)
public class DocumentMessageJsonWriter extends ExtensibleEntityJsonWriter<MessageRecord> {

    public DocumentMessageJsonWriter() {
        super(MessageRecord.class.getName(), MessageRecord.class);
    }

    @Override
    protected void writeEntityBody(MessageRecord entity, JsonGenerator jg) throws IOException {
        entity.getId()
        entity.writeExternal(jg);
    }
}
