package com.github.rahulsom.orri;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parsed representation of a supported {@code DROP TABLE} or {@code DROP VIEW} statement.
 */
record DropRelationSpec(
        Type type,
        String relationName) {
    private static final Pattern DROP_PATTERN = Pattern.compile(
            "^\\s*drop\\s+(table|view)\\s+(?:if\\s+exists\\s+)?(\"(?:[^\"]|\"\")*\"|[^\\s;]+)",
            Pattern.CASE_INSENSITIVE);

    static DropRelationSpec parse(String sql) {
        if (sql == null) {
            return null;
        }
        Matcher matcher = DROP_PATTERN.matcher(sql);
        if (!matcher.find()) {
            return null;
        }
        Type type = "view".equalsIgnoreCase(matcher.group(1)) ? Type.VIEW : Type.TABLE;
        return new DropRelationSpec(type, SqlMutation.normalizeIdentifier(matcher.group(2)));
    }

    enum Type {
        TABLE,
        VIEW
    }
}
