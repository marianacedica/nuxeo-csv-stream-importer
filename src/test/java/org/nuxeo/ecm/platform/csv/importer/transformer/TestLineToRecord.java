package org.nuxeo.ecm.platform.csv.importer.transformer;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.platform.csv.importer.transformer.bean.LineBean;
import org.nuxeo.ecm.platform.test.PlatformFeature;
import org.nuxeo.importer.stream.message.DocumentMessage;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;

import javax.inject.Inject;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertNotNull;

@RunWith(FeaturesRunner.class)
@Features({PlatformFeature.class})
@Deploy("org.nuxeo.importer.stream.csv")
public class TestLineToRecord {

    private static final List<String> HEADERS = Arrays.asList(
            "id",
            "type",
            "dc:title",
            "CurrentVersion",
            "FoldersFiledIn",
            "dc:created",
            "dc:creator",
            "dc:lastContributor",
            "file.content/length",
            "file.content/mime-type",
            "file.content/name",
            "lastModified",
            "uid:major_version",
            "uid:minor_version"
    );

    @Inject
    protected LineToRecord linetorecord;

    @Test
    public void testService() {
        assertNotNull(linetorecord);
    }

    @Test
    public void testParseProperties() {
        final List<String> VALUES = Arrays.asList(
                /*"id",*/                       "123",
                /*"type",*/                     "File",
                /*"dc:title",*/                 "A Title",
                /*"CurrentVersion",*/           "",
                /*"FoldersFiledIn",*/           "",
                /*"dc:created",*/               "2007-06-25T17:13:05",
                /*"dc:creator",*/               "Administrator",
                /*"dc:lastContributor",*/       "Administrator",
                /*"file.content/length",*/      "12",
                /*"file.content/mime-type",*/   "application/pdf",
                /*"file.content/name",*/        "test.pdf",
                /*"lastModified",*/             "2007-06-25T17:13:05",
                /*"uid:major_version",*/        "1",
                /*"uid:minor_version"*/         "1");

        /*
        // FUTURE STATE?
        final HashMap<String, Serializable> EXPECTED = new HashMap<String, Serializable>() {{
            put("id", "123");
            put("type", "File");
            put("dc:title", "A Title");
            put("CurrentVersion", "");
            put("FoldersFiledIn", "");
            put("dc:created", "2007-06-25T17:13:05");
            put("dc:creator", "Administrator");
            put("dc:lastContributor", "Administrator");
            put("file.content/length", "12");
            put("file.content/mime-type", "application/pdf");
            put("file.content/name", "test.pdf");
            put("lastModified", "2007-06-25T17:13:05");
            put("uid:major_version", "1");
            put("uid:minor_version", "1");
        }};
        */

        final HashMap<String, Serializable> EXPECTED = new HashMap<String, Serializable>() {{
            put("dc:title", "A Title");
            put("dc:created", "2007-06-25T17:13:05");
            put("dc:creator", "Administrator");
            put("dc:lastContributor", "Administrator");
            put("uid:major_version", "1");
            put("uid:minor_version", "1");
        }};

        LineBean lineBean = new LineBean(HEADERS, VALUES, 1L, "path");
        // WHEN
        HashMap<String, Serializable> result = LineToRecordImpl.parseProperties(lineBean);

        // THEN
        Assertions.assertThat(result).isEqualTo(EXPECTED);
    }

    @Test
    public void testParsePropertiesEmptyLists() {
        // GIVEN
        final HashMap<String, Serializable> EXPECTED = new HashMap<>();
        LineBean lineBean = new LineBean(Collections.emptyList(), Collections.emptyList(), 1L, "path");

        // WHEN
        HashMap<String, Serializable> result = LineToRecordImpl.parseProperties(lineBean);

        // THEN
        Assertions.assertThat(result).isEqualTo(EXPECTED);
    }

    @Test
    public void testLineToDocMessageWithValues() {
        final List<String> VALUES = Arrays.asList(
                /*"id",*/                       "123",
                /*"type",*/                     "File",
                /*"dc:title",*/                 "A Title",
                /*"CurrentVersion",*/           "",
                /*"FoldersFiledIn",*/           "/Folder-Name", // NationWide specific
                /*"dc:created",*/               "2007-06-25T17:13:05",
                /*"dc:creator",*/               "Administrator",
                /*"dc:lastContributor",*/       "Administrator",
                /*"file.content/length",*/      "12",
                /*"file.content/mime-type",*/   "application/pdf",
                /*"file.content/name",*/        "test.pdf",
                /*"lastModified",*/             "2007-06-25T17:13:05",
                /*"uid:major_version",*/        "1",
                /*"uid:minor_version"*/         "1");

        final DocumentMessage EXPECTED = DocumentMessage.builder("File", "/Folder-Name", "A Title")
                .setProperties(new HashMap<String, Serializable>() {{
                    put("dc:title", "A Title");
                    put("dc:created", "2007-06-25T17:13:05");
                    put("dc:creator", "Administrator");
                    put("dc:lastContributor", "Administrator");
                    put("uid:major_version", "1");
                    put("uid:minor_version", "1");
                }})
                .build();

        LineBean lineBean = new LineBean(HEADERS, VALUES, 1L, "path.csv");
        // WHEN
        DocumentMessage result = LineToRecordImpl.LINE_TO_DOCMESSAGE.apply(lineBean);

        // THEN
        Assertions.assertThat(result).isEqualToComparingFieldByField(EXPECTED);
    }


    @Test
    public void testLineToDocMessageWithNoValues() {
        // GIVEN
        LineBean lineBean = new LineBean(Collections.emptyList(), Collections.emptyList(), 1L, "path");
        // WHEN
        DocumentMessage result = LineToRecordImpl.LINE_TO_DOCMESSAGE.apply(lineBean);
        // THEN
        Assertions.assertThat(result).isNull();
    }


    @Test
    public void testLineToDocMessageWithNullValues() {
        // GIVEN
        LineBean lineBean = new LineBean(null, null, 1L, "path");
        // WHEN
        DocumentMessage result = LineToRecordImpl.LINE_TO_DOCMESSAGE.apply(lineBean);
        // THEN
        Assertions.assertThat(result).isNull();
    }
}
