package com.github.rahulsom.orri;

import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;

/**
 * Parsed representation of a {@code jdbc:orri:} connection URL.
 */
record OrriJdbcUrl(
        String originalUrl,
        String spreadsheetId,
        Properties properties) {

    static final String PREFIX = "jdbc:orri:";

    static OrriJdbcUrl parse(String url, Properties connectionProperties) throws SQLException {
        if (url == null || !url.startsWith(PREFIX)) {
            throw new SQLException("Orri JDBC URLs must start with " + PREFIX);
        }

        String remainder = url.substring(PREFIX.length());
        String[] segments = remainder.split(";");
        String spreadsheetId = segments[0].trim();
        if (spreadsheetId.isEmpty()) {
            throw new SQLException("Missing spreadsheet id in JDBC URL: " + url);
        }

        Properties merged = new Properties();
        for (int index = 1; index < segments.length; index++) {
            String segment = segments[index].trim();
            if (segment.isEmpty()) {
                continue;
            }

            int separator = segment.indexOf('=');
            if (separator <= 0 || separator == segment.length() - 1) {
                throw new SQLException("Invalid Orri JDBC URL property: " + segment);
            }

            merged.setProperty(segment.substring(0, separator), segment.substring(separator + 1));
        }

        if (connectionProperties != null) {
            for (String name : connectionProperties.stringPropertyNames()) {
                merged.setProperty(name, connectionProperties.getProperty(name));
            }
        }

        return new OrriJdbcUrl(url, spreadsheetId, merged);
    }

    String property(String name) {
        return properties.getProperty(name);
    }

    String applicationName() {
        return Objects.requireNonNullElse(property("applicationName"), "orri");
    }

    boolean readOnly() {
        return "true".equalsIgnoreCase(property("readOnly"));
    }
}
