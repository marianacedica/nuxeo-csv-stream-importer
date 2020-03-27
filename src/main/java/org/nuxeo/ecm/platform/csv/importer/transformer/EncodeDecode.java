package org.nuxeo.ecm.platform.csv.importer.transformer;

import org.nuxeo.importer.stream.message.DocumentMessage;
import org.nuxeo.lib.stream.codec.Codec;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.codec.CodecService;

import java.util.function.Function;

public class EncodeDecode {

    //DO NOT USE avro, it will fail while encoding, the codec does not support null values (for ex, when type Folder and no blob or blobinfo)
    private static final String DEFAULT_CODEC = "java";

    public static Codec<DocumentMessage> getCommandCodec() {
        return Framework.getService(CodecService.class).getCodec(DEFAULT_CODEC, DocumentMessage.class);
    }

    public static Function<byte[], DocumentMessage> decode = (record) -> getCommandCodec().decode(record);

    public static Function<DocumentMessage, byte[]> encode = (documentMessage) -> getCommandCodec().encode(documentMessage);

}
