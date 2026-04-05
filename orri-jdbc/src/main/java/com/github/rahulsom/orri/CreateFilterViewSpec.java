package com.github.rahulsom.orri;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parsed representation of a Sheets-compatible {@code CREATE VIEW} statement.
 */
record CreateFilterViewSpec(
        String viewName,
        WorksheetSnapshot sourceWorksheet,
        int startColumnIndex,
        Integer endColumnIndex,
        List<FilterCriterion> criteria,
        List<SortKey> sortKeys) {
    private static final Pattern CREATE_VIEW_PATTERN = Pattern.compile(
            "(?is)^\\s*create\\s+view\\s+(?:if\\s+not\\s+exists\\s+)?(?<name>\"(?:[^\"]|\"\")*\"|\\S+)"
                    + "\\s+as\\s+select\\s+(?<select>.+?)\\s+from\\s+(?<from>\"(?:[^\"]|\"\")*\"|\\S+)"
                    + "(?:\\s+where\\s+(?<where>.+?))?"
                    + "(?:\\s+order\\s+by\\s+(?<order>.+?))?\\s*$");
    private static final Pattern COMPARISON_PATTERN = Pattern.compile(
            "(?is)^\\s*(\"(?:[^\"]|\"\")*\"|[^\\s]+)\\s*(=|>=|<=|>|<)\\s*(.+?)\\s*$");
    private static final Pattern LIKE_PATTERN = Pattern.compile(
            "(?is)^\\s*(\"(?:[^\"]|\"\")*\"|[^\\s]+)\\s+like\\s+(.+?)\\s*$");
    private static final Pattern NULL_PATTERN = Pattern.compile(
            "(?is)^\\s*(\"(?:[^\"]|\"\")*\"|[^\\s]+)\\s+is\\s+(not\\s+)?null\\s*$");

    static CreateFilterViewSpec parse(String sql, SpreadsheetSnapshot snapshot) throws SQLException {
        if (sql == null) {
            return null;
        }

        Matcher matcher = CREATE_VIEW_PATTERN.matcher(sql);
        if (!matcher.find()) {
            return null;
        }

        String viewName = SqlMutation.normalizeIdentifier(matcher.group("name"));
        String sourceName = SqlMutation.normalizeIdentifier(matcher.group("from"));
        WorksheetSnapshot sourceWorksheet = snapshot.worksheet(sourceName);
        if (sourceWorksheet == null) {
            throw new SQLException("CREATE VIEW is only supported from worksheet tables");
        }

        List<String> selectedColumns = parseSelectedColumns(matcher.group("select"), sourceWorksheet);
        int startColumnIndex = startColumnIndex(sourceWorksheet, selectedColumns);
        int endColumnIndex = startColumnIndex + selectedColumns.size();
        List<FilterCriterion> criteria = parseCriteria(matcher.group("where"), sourceWorksheet);
        List<SortKey> sortKeys = parseSortKeys(matcher.group("order"), sourceWorksheet);

        return new CreateFilterViewSpec(
                viewName,
                sourceWorksheet,
                startColumnIndex,
                endColumnIndex,
                List.copyOf(criteria),
                List.copyOf(sortKeys));
    }

    FilterViewDefinition toFilterViewDefinition() {
        return new FilterViewDefinition(
                null,
                viewName,
                sourceWorksheet.sheetId(),
                0,
                null,
                startColumnIndex,
                endColumnIndex,
                criteria,
                sortKeys);
    }

    private static List<String> parseSelectedColumns(String selectClause, WorksheetSnapshot sourceWorksheet) throws SQLException {
        String trimmed = selectClause.trim();
        if ("*".equals(trimmed)) {
            return sourceWorksheet.columnNames();
        }

        List<String> columns = new ArrayList<>();
        for (String column : splitCommaSeparated(trimmed)) {
            columns.add(simpleIdentifier(column.trim()));
        }
        if (columns.isEmpty()) {
            throw new SQLException("CREATE VIEW must select at least one column");
        }
        for (String column : columns) {
            if (!sourceWorksheet.columnNames().contains(column)) {
                throw new SQLException("Unknown column " + column + " in CREATE VIEW");
            }
        }
        return columns;
    }

    private static int startColumnIndex(WorksheetSnapshot worksheet, List<String> selectedColumns) throws SQLException {
        List<Integer> indexes = selectedColumns.stream()
                .map(worksheet.columnNames()::indexOf)
                .sorted()
                .toList();
        int start = indexes.getFirst();
        for (int offset = 0; offset < indexes.size(); offset++) {
            if (indexes.get(offset) != start + offset) {
                throw new SQLException("CREATE VIEW only supports contiguous selected columns");
            }
        }
        return start;
    }

    private static List<FilterCriterion> parseCriteria(String whereClause, WorksheetSnapshot worksheet) throws SQLException {
        if (whereClause == null || whereClause.isBlank()) {
            return List.of();
        }

        List<FilterCriterion> criteria = new ArrayList<>();
        for (String part : splitLogicalAnd(whereClause)) {
            criteria.add(parseCriterion(part, worksheet));
        }
        return criteria;
    }

    private static FilterCriterion parseCriterion(String clause, WorksheetSnapshot worksheet) throws SQLException {
        Matcher nullMatcher = NULL_PATTERN.matcher(clause);
        if (nullMatcher.find()) {
            String columnName = simpleIdentifier(nullMatcher.group(1));
            int columnIndex = columnIndex(worksheet, columnName);
            boolean not = nullMatcher.group(2) != null;
            return new FilterCriterion(columnIndex, List.of(), not ? "NOT_BLANKS" : "BLANKS", List.of());
        }

        Matcher likeMatcher = LIKE_PATTERN.matcher(clause);
        if (likeMatcher.find()) {
            String columnName = simpleIdentifier(likeMatcher.group(1));
            int columnIndex = columnIndex(worksheet, columnName);
            String value = parseLiteral(likeMatcher.group(2));
            if (value.startsWith("%") && value.endsWith("%") && value.length() >= 2) {
                return new FilterCriterion(columnIndex, List.of(), "TEXT_CONTAINS", List.of(value.substring(1, value.length() - 1)));
            }
            if (value.endsWith("%")) {
                return new FilterCriterion(columnIndex, List.of(), "TEXT_STARTS_WITH", List.of(value.substring(0, value.length() - 1)));
            }
            if (value.startsWith("%")) {
                return new FilterCriterion(columnIndex, List.of(), "TEXT_ENDS_WITH", List.of(value.substring(1)));
            }
            throw new SQLException("Unsupported LIKE pattern in CREATE VIEW: " + clause);
        }

        Matcher comparisonMatcher = COMPARISON_PATTERN.matcher(clause);
        if (comparisonMatcher.find()) {
            String columnName = simpleIdentifier(comparisonMatcher.group(1));
            int columnIndex = columnIndex(worksheet, columnName);
            String operator = comparisonMatcher.group(2);
            String literal = parseLiteral(comparisonMatcher.group(3));
            ColumnType columnType = worksheet.columnTypes().get(columnIndex);
            if (columnType == ColumnType.BOOLEAN) {
                if (!"=".equals(operator)) {
                    throw new SQLException("Boolean filters only support equality in CREATE VIEW");
                }
                return new FilterCriterion(columnIndex, booleanHiddenValues(literal), null, List.of());
            }
            if (columnType == ColumnType.DECIMAL) {
                return new FilterCriterion(columnIndex, List.of(), numericCondition(operator), List.of(literal));
            }
            if (!"=".equals(operator)) {
                throw new SQLException("Text filters only support equality in CREATE VIEW");
            }
            return new FilterCriterion(columnIndex, List.of(), "TEXT_EQ", List.of(literal));
        }

        throw new SQLException("Unsupported WHERE clause in CREATE VIEW: " + clause);
    }

    private static List<SortKey> parseSortKeys(String orderClause, WorksheetSnapshot worksheet) throws SQLException {
        if (orderClause == null || orderClause.isBlank()) {
            return List.of();
        }
        List<SortKey> sortKeys = new ArrayList<>();
        for (String part : splitCommaSeparated(orderClause)) {
            String[] pieces = part.trim().split("\\s+");
            if (pieces.length == 0 || pieces.length > 2) {
                throw new SQLException("Unsupported ORDER BY clause in CREATE VIEW: " + part);
            }
            String columnName = simpleIdentifier(pieces[0]);
            int columnIndex = columnIndex(worksheet, columnName);
            boolean descending = pieces.length == 2 && "DESC".equalsIgnoreCase(pieces[1]);
            if (pieces.length == 2 && !descending && !"ASC".equalsIgnoreCase(pieces[1])) {
                throw new SQLException("Unsupported ORDER BY direction in CREATE VIEW: " + part);
            }
            sortKeys.add(new SortKey(columnIndex, descending));
        }
        return sortKeys;
    }

    private static String numericCondition(String operator) throws SQLException {
        return switch (operator) {
            case "=" -> "NUMBER_EQ";
            case ">" -> "NUMBER_GREATER";
            case ">=" -> "NUMBER_GREATER_THAN_EQ";
            case "<" -> "NUMBER_LESS";
            case "<=" -> "NUMBER_LESS_THAN_EQ";
            default -> throw new SQLException("Unsupported numeric comparison: " + operator);
        };
    }

    private static List<String> booleanHiddenValues(String literal) throws SQLException {
        return switch (literal.toLowerCase(Locale.ROOT)) {
            case "true" -> List.of("FALSE", "false");
            case "false" -> List.of("TRUE", "true");
            default -> throw new SQLException("Boolean filters only support true and false literals in CREATE VIEW");
        };
    }

    private static int columnIndex(WorksheetSnapshot worksheet, String columnName) throws SQLException {
        int index = worksheet.columnNames().indexOf(columnName);
        if (index < 0) {
            throw new SQLException("Unknown column " + columnName + " in CREATE VIEW");
        }
        return index;
    }

    private static String simpleIdentifier(String value) throws SQLException {
        String normalized = SqlMutation.normalizeIdentifier(value.trim());
        if (normalized.contains(" ")) {
            return normalized;
        }
        if (normalized.contains("(") || normalized.contains(")")) {
            throw new SQLException("CREATE VIEW only supports simple column references");
        }
        return normalized;
    }

    private static String parseLiteral(String literal) {
        String value = literal.trim();
        if (value.startsWith("'") && value.endsWith("'")) {
            return value.substring(1, value.length() - 1).replace("''", "'");
        }
        return value;
    }

    private static List<String> splitCommaSeparated(String value) {
        return List.of(value.split("\\s*,\\s*"));
    }

    private static List<String> splitLogicalAnd(String value) {
        return List.of(value.split("(?i)\\s+and\\s+"));
    }
}
