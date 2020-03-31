package org.nuxeo.ecm.platform.csv.importer.transformer.bean;

import java.util.List;
import java.util.Objects;

public class LineBean {
    private List<String> headers;
    private List<String> columns;
    private Long lineNumber;
    private String csvPath;

    public LineBean(List<String> headers, List<String> columns, Long lineNumber, String csvPath) {
        this.headers = headers;
        this.columns = columns;
        this.lineNumber = lineNumber;
        this.csvPath = csvPath;
    }

    public List<String> getHeaders() {
        return headers;
    }

    public List<String> getColumns() {
        return columns;
    }

    public Long getLineNumber() {
        return lineNumber;
    }

    public String getCsvPath() {
        return csvPath;
    }

    public boolean hasValidData() {
        return this.headers != null &&
                !this.headers.isEmpty() &&
                // Column need to be greater than 5 to make sure we capture everything
                this.columns != null &&
                !this.columns.isEmpty() &&
                this.columns.size() > 5 &&
                // Size of both header and columns has to be equal
                this.columns.size() == this.headers.size() &&
                this.lineNumber != null &&
                this.csvPath != null;
    }

    @Override
    public String toString() {
        return "LineBean{" +
                "headers=" + headers +
                ", columns=" + columns +
                ", lineNumber=" + lineNumber +
                ", csvPath='" + csvPath + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LineBean)) {
            return false;
        }
        LineBean lineBean = (LineBean) o;
        return Objects.equals(headers, lineBean.headers) &&
                Objects.equals(columns, lineBean.columns) &&
                Objects.equals(lineNumber, lineBean.lineNumber) &&
                Objects.equals(csvPath, lineBean.csvPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(headers, columns, lineNumber, csvPath);
    }
}
