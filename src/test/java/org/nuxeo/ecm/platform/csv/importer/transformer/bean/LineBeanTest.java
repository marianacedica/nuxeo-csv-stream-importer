package org.nuxeo.ecm.platform.csv.importer.transformer.bean;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public class LineBeanTest {

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

    final List<String> VALUES_WITH_MINIMUM = Arrays.asList(
            /*"id",*/                       "123",
            /*"type",*/                     "File",
            /*"dc:title",*/                 "A Title",
            /*"CurrentVersion",*/           "",
            /*"FoldersFiledIn",*/           "/Folder-Name",
            /*"dc:created",*/               "2007-06-25T17:13:05");

    @Test
    public void testHasValidDataEmptyHeaderAndColumns() {

        // GIVEN
        LineBean lineBean = new LineBean(Collections.emptyList(), Collections.emptyList(), 1L, "path");
        // WHEN
        boolean result = lineBean.hasValidData();
        // THEN
        Assertions.assertThat(result).isFalse();
    }

    @Test
    public void testHasValidDataHeaderAndColumnsWithMinimunValues() {

        // GIVEN
        LineBean lineBean = new LineBean(VALUES_WITH_MINIMUM, VALUES_WITH_MINIMUM, 1L, "path");
        // WHEN
        boolean result = lineBean.hasValidData();
        // THEN
        Assertions.assertThat(result).isTrue();
    }

    @Test
    public void testHasValidDataHeaderAndColumns() {

        // GIVEN
        LineBean lineBean = new LineBean(VALUES, VALUES, 1L, "path");
        // WHEN
        boolean result = lineBean.hasValidData();
        // THEN
        Assertions.assertThat(result).isTrue();
    }

    @Test
    public void testHasValidDataHeaderAndColumnsButDifferentSize() {

        // GIVEN
        LineBean lineBean = new LineBean(Arrays.asList("a", "b"), VALUES, 1L, "path");
        // WHEN
        boolean result = lineBean.hasValidData();
        // THEN
        Assertions.assertThat(result).isFalse();
    }


    @Test
    public void testHasValidDataAllFieldsNull() {
        // GIVEN
        LineBean lineBean = new LineBean(null, null, null, null);
        // WHEN
        boolean result = lineBean.hasValidData();
        // THEN
        Assertions.assertThat(result).isFalse();
    }
}
