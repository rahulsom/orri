package io.github.rahulsom.orri;

import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parsed representation of a supported {@code ALTER VIEW} statement.
 */
record AlterViewSpec(
        Type type, String viewName, String targetName, CreateFilterViewSpec replacementSpec, String createSql) {
    private static final Pattern RENAME_VIEW_PATTERN = Pattern.compile(
            "^\\s*alter\\s+view\\s+(\"(?:[^\"]|\"\")*\"|[^\\s]+)\\s+rename\\s+to\\s+(\"(?:[^\"]|\"\")*\"|[^\\s;]+)\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern REPLACE_VIEW_PATTERN = Pattern.compile(
            "^\\s*alter\\s+view\\s+(\"(?:[^\"]|\"\")*\"|[^\\s]+)\\s+as\\s+(.+)$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    static AlterViewSpec parse(String sql, SpreadsheetSnapshot snapshot) throws SQLException {
        if (sql == null) {
            return null;
        }

        Matcher matcher = RENAME_VIEW_PATTERN.matcher(sql);
        if (matcher.find()) {
            return new AlterViewSpec(
                    Type.RENAME_VIEW,
                    SqlMutation.normalizeIdentifier(matcher.group(1)),
                    SqlMutation.normalizeIdentifier(matcher.group(2)),
                    null,
                    null);
        }

        matcher = REPLACE_VIEW_PATTERN.matcher(sql);
        if (matcher.find()) {
            String viewName = SqlMutation.normalizeIdentifier(matcher.group(1));
            String createSql = "create view " + quoteIdentifier(viewName) + " as "
                    + matcher.group(2).trim();
            return new AlterViewSpec(
                    Type.REPLACE_VIEW, viewName, null, CreateFilterViewSpec.parse(createSql, snapshot), createSql);
        }

        return null;
    }

    private static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    enum Type {
        RENAME_VIEW,
        REPLACE_VIEW
    }
}
