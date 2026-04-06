package io.github.rahulsom.orri;

import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parsed representation of a supported {@code ALTER TABLE} statement.
 */
record AlterTableSpec(Type type, String relationName, String subjectName, String targetName) {
    private static final Pattern RENAME_TABLE_PATTERN = Pattern.compile(
            "^\\s*alter\\s+table\\s+(\"(?:[^\"]|\"\")*\"|[^\\s]+)\\s+rename\\s+to\\s+(\"(?:[^\"]|\"\")*\"|[^\\s;]+)\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern ADD_COLUMN_PATTERN = Pattern.compile(
            "^\\s*alter\\s+table\\s+(\"(?:[^\"]|\"\")*\"|[^\\s]+)\\s+add\\s+column\\s+(\"(?:[^\"]|\"\")*\"|[^\\s]+)\\s+.+$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern DROP_COLUMN_PATTERN = Pattern.compile(
            "^\\s*alter\\s+table\\s+(\"(?:[^\"]|\"\")*\"|[^\\s]+)\\s+drop\\s+column\\s+(\"(?:[^\"]|\"\")*\"|[^\\s;]+)\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern RENAME_COLUMN_PATTERN = Pattern.compile(
            "^\\s*alter\\s+table\\s+(\"(?:[^\"]|\"\")*\"|[^\\s]+)\\s+alter\\s+column\\s+(\"(?:[^\"]|\"\")*\"|[^\\s]+)\\s+rename\\s+to\\s+(\"(?:[^\"]|\"\")*\"|[^\\s;]+)\\s*$",
            Pattern.CASE_INSENSITIVE);

    static AlterTableSpec parse(String sql) throws SQLException {
        if (sql == null) {
            return null;
        }

        Matcher matcher = RENAME_TABLE_PATTERN.matcher(sql);
        if (matcher.find()) {
            return new AlterTableSpec(
                    Type.RENAME_TABLE,
                    SqlMutation.normalizeIdentifier(matcher.group(1)),
                    null,
                    SqlMutation.normalizeIdentifier(matcher.group(2)));
        }

        matcher = ADD_COLUMN_PATTERN.matcher(sql);
        if (matcher.find()) {
            return new AlterTableSpec(
                    Type.ADD_COLUMN,
                    SqlMutation.normalizeIdentifier(matcher.group(1)),
                    SqlMutation.normalizeIdentifier(matcher.group(2)),
                    null);
        }

        matcher = DROP_COLUMN_PATTERN.matcher(sql);
        if (matcher.find()) {
            return new AlterTableSpec(
                    Type.DROP_COLUMN,
                    SqlMutation.normalizeIdentifier(matcher.group(1)),
                    SqlMutation.normalizeIdentifier(matcher.group(2)),
                    null);
        }

        matcher = RENAME_COLUMN_PATTERN.matcher(sql);
        if (matcher.find()) {
            return new AlterTableSpec(
                    Type.RENAME_COLUMN,
                    SqlMutation.normalizeIdentifier(matcher.group(1)),
                    SqlMutation.normalizeIdentifier(matcher.group(2)),
                    SqlMutation.normalizeIdentifier(matcher.group(3)));
        }

        return null;
    }

    enum Type {
        RENAME_TABLE,
        ADD_COLUMN,
        DROP_COLUMN,
        RENAME_COLUMN
    }
}
