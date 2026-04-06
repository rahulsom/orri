package io.github.rahulsom.orri;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Minimal SQL mutation descriptor used to detect worksheet write operations.
 */
record SqlMutation(Type type, String relationName) {
    private static final Pattern INSERT_PATTERN =
            Pattern.compile("^\\s*insert\\s+into\\s+(\"(?:[^\"]|\"\")*\"|[^\\s(]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern UPDATE_PATTERN =
            Pattern.compile("^\\s*update\\s+(\"(?:[^\"]|\"\")*\"|[^\\s]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern DELETE_PATTERN =
            Pattern.compile("^\\s*delete\\s+from\\s+(\"(?:[^\"]|\"\")*\"|[^\\s]+)", Pattern.CASE_INSENSITIVE);

    static SqlMutation parse(String sql) {
        if (sql == null || sql.isBlank()) {
            return null;
        }

        SqlMutation insert = fromMatcher(Type.INSERT, INSERT_PATTERN.matcher(sql));
        if (insert != null) {
            return insert;
        }

        SqlMutation update = fromMatcher(Type.UPDATE, UPDATE_PATTERN.matcher(sql));
        if (update != null) {
            return update;
        }

        return fromMatcher(Type.DELETE, DELETE_PATTERN.matcher(sql));
    }

    private static SqlMutation fromMatcher(Type type, Matcher matcher) {
        if (!matcher.find()) {
            return null;
        }
        return new SqlMutation(type, normalizeIdentifier(matcher.group(1)));
    }

    static String normalizeIdentifier(String identifier) {
        String value = identifier;
        int separator = value.lastIndexOf('.');
        if (separator >= 0) {
            value = value.substring(separator + 1);
        }
        if (value.startsWith("\"") && value.endsWith("\"")) {
            return value.substring(1, value.length() - 1).replace("\"\"", "\"");
        }
        return value.toLowerCase(Locale.ROOT);
    }

    enum Type {
        INSERT,
        UPDATE,
        DELETE
    }
}
