package org.nuxeo.ecm.platform.csv.importer.transformer;

import org.nuxeo.ecm.platform.csv.importer.transformer.bean.LineBean;
import org.nuxeo.lib.stream.computation.Record;

import java.util.function.Function;

public interface LineToRecord {

    Function<LineBean, Record> transform();
}
