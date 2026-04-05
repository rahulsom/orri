package com.github.rahulsom.orri;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parsed representation of a supported {@code CREATE TABLE} statement.
 */
record CreateTableSpec(String relationName) {
    private static final Pattern CREATE_TABLE_PATTERN = Pattern.compile(
            "^\\s*create\\s+table\\s+(?:if\\s+not\\s+exists\\s+)?(\"(?:[^\"]|\"\")*\"|[^\\s(]+)",
            Pattern.CASE_INSENSITIVE);

    static CreateTableSpec parse(String sql) {
        if (sql == null) {
            return null;
        }
        Matcher matcher = CREATE_TABLE_PATTERN.matcher(sql);
        if (!matcher.find()) {
            return null;
        }
        return new CreateTableSpec(SqlMutation.normalizeIdentifier(matcher.group(1)));
    }
}
