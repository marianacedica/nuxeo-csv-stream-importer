package org.nuxeo.ecm.platform.csv.importer.processor.computation;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.PathRef;
import org.nuxeo.ecm.core.api.impl.blob.StringBlob;
import org.nuxeo.ecm.core.api.model.PropertyNotFoundException;
import org.nuxeo.ecm.core.blob.BlobInfo;
import org.nuxeo.ecm.platform.test.PlatformFeature;
import org.nuxeo.importer.stream.message.DocumentMessage;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;

import javax.inject.Inject;
import java.io.Serializable;
import java.util.HashMap;

import static org.junit.Assert.fail;

@RunWith(FeaturesRunner.class)
@Features({PlatformFeature.class})
@Deploy("org.nuxeo.importer.stream.csv")
public class DocumentCreationTest {

    private static final String DC_TITLE = "dc:title";
    private static final String DC_DESCRIPTION = "dc:description";
    private static final String WHAOU = "whaou";
    private static final String NICE_FOLDER = "nice folder";
    private static final BlobInfo BLOB_INFO = new BlobInfo();
    private static final StringBlob HELLO = new StringBlob("hello");
    @Inject
    CoreSession session;

    @Test
    public void createDocument() {

        // GIVEN
        final DocumentMessage input = DocumentMessage.builder("File", "/", "--name--")
                .setProperties(new HashMap<String, Serializable>() {{
                    put(DC_TITLE, WHAOU);
                    put(DC_DESCRIPTION, NICE_FOLDER);
                }})
                .setBlob(HELLO)
                .setBlobInfo(BLOB_INFO)
                .build();

        // WHEN
        DocumentCreation.createDocument(session, input);

        // THEN
        DocumentModel result = session.getDocument(new PathRef("/--name--"));
        Assertions.assertThat(result.getPropertyValue(DC_TITLE)).isEqualTo(WHAOU);
        Assertions.assertThat(result.getPropertyValue(DC_DESCRIPTION)).isEqualTo(NICE_FOLDER);
        Assertions.assertThat((Blob) result.getPropertyValue("file:content")).isNotNull();
    }

    @Test
    public void testCraftBlobWithBlobAndBlobInfoValues() {


        // GIVEN
        final DocumentMessage input = DocumentMessage.builder("File", "/", "--name--")
                .setProperties(new HashMap<String, Serializable>() {{
                    put(DC_TITLE, WHAOU);
                    put(DC_DESCRIPTION, NICE_FOLDER);
                }})
                .setBlob(HELLO)
                .setBlobInfo(BLOB_INFO)
                .build();

        // WHEN
        Blob result = DocumentCreation.craftBlob(input);

        // THEN
        Assertions.assertThat(result).isNotNull();
    }

    @Test
    public void testCraftBlobWithValuesWithBlobInfoOnly() {


        // GIVEN
        final DocumentMessage input = DocumentMessage.builder("File", "/", "--name--")
                .setProperties(new HashMap<String, Serializable>() {{
                    put(DC_TITLE, WHAOU);
                    put(DC_DESCRIPTION, NICE_FOLDER);
                }})
                .setBlobInfo(BLOB_INFO)
                .build();

        // WHEN
        Blob result = DocumentCreation.craftBlob(input);

        // THEN
        Assertions.assertThat(result).isNotNull();
    }

    @Test
    public void testCraftBlobWithNoValues() {

        // GIVEN
        final DocumentMessage input = DocumentMessage.builder("File", "/", "--name--")
                .setProperties(new HashMap<String, Serializable>() {{
                    put(DC_TITLE, WHAOU);
                    put(DC_DESCRIPTION, NICE_FOLDER);
                }})
                .build();

        // WHEN
        Blob result = DocumentCreation.craftBlob(input);

        // THEN
        Assertions.assertThat(result).isNull();
    }

    @Test
    public void testAddingPropertyValuesWithNonEmptyHashMap() {

        // GIVEN
        DocumentModel input = session.createDocumentModel("/", "--name--", "File");
        session.createDocument(input);
        HashMap<String, Serializable> extraProperties = new HashMap<String, Serializable>() {{
            put(DC_TITLE, WHAOU);
            put(DC_DESCRIPTION, NICE_FOLDER);
        }};
        // WHEN
        DocumentCreation.addingPropertyValues(input, extraProperties);
        session.saveDocument(input);

        // THEN
        DocumentModel result = session.getDocument(new PathRef("/--name--"));
        Assertions.assertThat(result.getPropertyValue(DC_TITLE)).isEqualTo(WHAOU);
        Assertions.assertThat(result.getPropertyValue(DC_DESCRIPTION)).isEqualTo(NICE_FOLDER);
        Assertions.assertThat((Blob) result.getPropertyValue("file:content")).isNull();
    }

    @Test
    public void testAddingPropertyValuesWithEmptyHashMap() {

        // GIVEN
        DocumentModel input = session.createDocumentModel("/", "--name--", "File");
        session.createDocument(input);
        HashMap<String, Serializable> extraProperties = new HashMap<>();
        // WHEN
        DocumentCreation.addingPropertyValues(input, extraProperties);
        session.saveDocument(input);

        // THEN
        DocumentModel result = session.getDocument(new PathRef("/--name--"));
        Assertions.assertThat(result.getPropertyValue(DC_TITLE)).isNull();
        Assertions.assertThat(result.getPropertyValue(DC_DESCRIPTION)).isNull();
        Assertions.assertThat((Blob) result.getPropertyValue("file:content")).isNull();
    }

    @Test(expected = PropertyNotFoundException.class)
    public void testAddingPropertyValuesWithNonExistingProperty() {

        // GIVEN
        DocumentModel input = session.createDocumentModel("/", "--name--", "File");
        session.createDocument(input);
        HashMap<String, Serializable> extraProperties = new HashMap<String, Serializable>() {{
            put("whatever:property", WHAOU);
        }};
        // WHEN
        try {
            DocumentCreation.addingPropertyValues(input, extraProperties);
        } catch (PropertyNotFoundException ex) {
            // THEN
            Assertions.assertThat(ex.getMessage()).isEqualTo("No such schema, whatever:property");
            throw ex;
        }
        fail("Should not be able to add a value to a non existing schema");
    }
}
