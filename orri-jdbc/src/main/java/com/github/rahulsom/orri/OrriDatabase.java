package com.github.rahulsom.orri;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Materializes worksheet tables and filter views into an in-memory H2 database.
 */
final class OrriDatabase {
    private OrriDatabase() {
    }

    static Connection materialize(SpreadsheetSnapshot snapshot, boolean readOnly) throws SQLException {
        Connection connection = DriverManager.getConnection(
                "jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false");
        Set<String> relationNames = new HashSet<>();

        for (WorksheetSnapshot worksheet : snapshot.worksheets()) {
            if (!relationNames.add(worksheet.name())) {
                throw new SQLException("Duplicate table or view name: " + worksheet.name());
            }
            createWorksheetTable(connection, worksheet);
        }

        for (FilterViewDefinition filterView : snapshot.filterViews()) {
            if (!relationNames.add(filterView.name())) {
                throw new SQLException("Duplicate table or view name: " + filterView.name());
            }
            createFilterViewTable(connection, snapshot, filterView);
        }

        connection.setReadOnly(readOnly);
        return connection;
    }

    static WorksheetSnapshot readWorksheetSnapshot(Connection connection, String relationName, Integer sheetId) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        List<String> columnNames = new ArrayList<>();
        List<ColumnType> columnTypes = new ArrayList<>();

        try (ResultSet columns = metaData.getColumns(null, null, relationName, "%")) {
            while (columns.next()) {
                columnNames.add(columns.getString("COLUMN_NAME"));
                columnTypes.add(columnType(columns.getInt("DATA_TYPE")));
            }
        }

        if (columnNames.isEmpty()) {
            throw new SQLException("Unknown worksheet " + relationName);
        }

        List<WorksheetRow> rows = new ArrayList<>();
        String sql = "select " + selectColumns(columnNames) + " from " + quote(relationName);
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            int rowIndex = 1;
            while (resultSet.next()) {
                List<CellValue> cells = new ArrayList<>(columnNames.size());
                for (int columnIndex = 0; columnIndex < columnNames.size(); columnIndex++) {
                    Object value = resultSet.getObject(columnIndex + 1);
                    cells.add(fromJdbcValue(value, columnTypes.get(columnIndex)));
                }
                rows.add(new WorksheetRow(rowIndex++, List.copyOf(cells)));
            }
        }

        return new WorksheetSnapshot(
                sheetId == null ? -1 : sheetId,
                relationName,
                List.copyOf(columnNames),
                List.copyOf(columnTypes),
                List.copyOf(rows));
    }

    static void refreshFilterViews(Connection connection, SpreadsheetSnapshot snapshot, String worksheetName) throws SQLException {
        for (FilterViewDefinition filterView : snapshot.filterViewsForWorksheet(worksheetName)) {
            String relationType = relationType(connection, filterView.name());
            if ("VIEW".equalsIgnoreCase(relationType)) {
                // Locally created SQL views stay dynamic inside H2 and do not need rematerialization.
                continue;
            }
            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE IF EXISTS " + quote(filterView.name()));
            }
            createFilterViewTable(connection, refreshedSnapshot(connection, snapshot, worksheetName), filterView);
        }
    }

    private static String relationType(Connection connection, String relationName) throws SQLException {
        try (ResultSet tables = connection.getMetaData().getTables(null, null, relationName, null)) {
            while (tables.next()) {
                String schemaName = tables.getString("TABLE_SCHEM");
                if (!"INFORMATION_SCHEMA".equalsIgnoreCase(schemaName)) {
                    return tables.getString("TABLE_TYPE");
                }
            }
        }
        return null;
    }

    private static SpreadsheetSnapshot refreshedSnapshot(
            Connection connection,
            SpreadsheetSnapshot snapshot,
            String worksheetName) throws SQLException {
        WorksheetSnapshot originalWorksheet = snapshot.worksheet(worksheetName);
        if (originalWorksheet == null) {
            throw new SQLException("Unknown worksheet " + worksheetName);
        }
        WorksheetSnapshot currentWorksheet = readWorksheetSnapshot(connection, worksheetName, originalWorksheet.sheetId());
        return new SpreadsheetSnapshot(List.of(currentWorksheet), List.of());
    }

    private static CellValue fromJdbcValue(Object value, ColumnType columnType) {
        if (value == null) {
            return CellValue.blank();
        }

        return switch (columnType) {
            case BOOLEAN -> new CellValue(value, String.valueOf(value), ValueKind.BOOLEAN);
            case DECIMAL -> new CellValue(value, String.valueOf(value), ValueKind.NUMBER);
            case VARCHAR -> new CellValue(String.valueOf(value), String.valueOf(value), ValueKind.STRING);
        };
    }

    private static String selectColumns(List<String> columnNames) {
        return columnNames.stream()
                .map(OrriDatabase::quote)
                .collect(java.util.stream.Collectors.joining(", "));
    }

    private static ColumnType columnType(int jdbcType) {
        return switch (jdbcType) {
            case java.sql.Types.BOOLEAN, java.sql.Types.BIT -> ColumnType.BOOLEAN;
            case java.sql.Types.DECIMAL, java.sql.Types.NUMERIC, java.sql.Types.INTEGER, java.sql.Types.BIGINT,
                    java.sql.Types.SMALLINT, java.sql.Types.FLOAT, java.sql.Types.DOUBLE, java.sql.Types.REAL -> ColumnType.DECIMAL;
            default -> ColumnType.VARCHAR;
        };
    }

    private static void createWorksheetTable(Connection connection, WorksheetSnapshot worksheet) throws SQLException {
        List<List<Object>> rows = worksheet.rows().stream()
                .map(row -> coerceRow(row, worksheet.columnTypes()))
                .toList();
        createTable(connection, worksheet.name(), worksheet.columnNames(), worksheet.columnTypes(), rows);
    }

    private static void createFilterViewTable(
            Connection connection,
            SpreadsheetSnapshot snapshot,
            FilterViewDefinition filterView) throws SQLException {
        WorksheetSnapshot sourceWorksheet = snapshot.worksheets().stream()
                .filter(worksheet -> worksheet.sheetId() == filterView.sourceSheetId())
                .findFirst()
                .orElseThrow(() -> new SQLException("Missing worksheet for filter view " + filterView.name()));

        int startColumnIndex = Math.max(0, filterView.startColumnIndex());
        int endColumnIndex = filterView.endColumnIndex() == null
                ? sourceWorksheet.columnNames().size()
                : Math.min(filterView.endColumnIndex(), sourceWorksheet.columnNames().size());
        if (startColumnIndex >= endColumnIndex) {
            throw new SQLException("Filter view " + filterView.name() + " does not expose any columns");
        }

        List<WorksheetRow> filteredRows = sourceWorksheet.rows().stream()
                .filter(row -> matchesRange(filterView, row))
                .filter(row -> matchesCriteria(filterView, row))
                .sorted(comparator(filterView))
                .toList();

        List<String> columnNames = new ArrayList<>(sourceWorksheet.columnNames().subList(startColumnIndex, endColumnIndex));
        List<ColumnType> columnTypes = new ArrayList<>(sourceWorksheet.columnTypes().subList(startColumnIndex, endColumnIndex));
        List<List<Object>> rows = filteredRows.stream()
                .map(row -> {
                    List<Object> values = coerceRow(row, sourceWorksheet.columnTypes());
                    return (List<Object>) new ArrayList<>(values.subList(startColumnIndex, endColumnIndex));
                })
                .toList();
        createTable(connection, filterView.name(), columnNames, columnTypes, rows);
    }

    private static boolean matchesRange(FilterViewDefinition filterView, WorksheetRow row) {
        int startRowIndex = Math.max(1, filterView.startRowIndex());
        int endRowIndex = filterView.endRowIndex() == null ? Integer.MAX_VALUE : filterView.endRowIndex();
        return row.sheetRowIndex() >= startRowIndex && row.sheetRowIndex() < endRowIndex;
    }

    private static boolean matchesCriteria(FilterViewDefinition filterView, WorksheetRow row) {
        for (FilterCriterion criterion : filterView.criteria()) {
            if (!matchesCriterion(criterion, row)) {
                return false;
            }
        }
        return true;
    }

    private static boolean matchesCriterion(FilterCriterion criterion, WorksheetRow row) {
        CellValue cell = criterion.columnIndex() < row.cells().size()
                ? row.cells().get(criterion.columnIndex())
                : CellValue.blank();
        String displayValue = cell.displayValue() == null ? "" : cell.displayValue();

        if (!criterion.hiddenValues().isEmpty() && criterion.hiddenValues().contains(displayValue)) {
            return false;
        }

        String conditionType = criterion.conditionType();
        if (conditionType == null || conditionType.isBlank()) {
            return true;
        }

        return switch (conditionType) {
            case "BLANKS", "CELL_EMPTY" -> cell.kind() == ValueKind.BLANK;
            case "NOT_BLANKS", "CELL_NOT_EMPTY" -> cell.kind() != ValueKind.BLANK;
            case "TEXT_EQ" -> anyValueMatches(criterion.conditionValues(), displayValue::equals);
            case "TEXT_CONTAINS" -> anyValueMatches(criterion.conditionValues(), displayValue::contains);
            case "TEXT_STARTS_WITH" -> anyValueMatches(criterion.conditionValues(), displayValue::startsWith);
            case "TEXT_ENDS_WITH" -> anyValueMatches(criterion.conditionValues(), displayValue::endsWith);
            case "TEXT_IS_EMAIL" -> displayValue.contains("@");
            case "TEXT_IS_URL" -> //noinspection HttpUrlsUsage
                    displayValue.startsWith("http://") || displayValue.startsWith("https://");
            case "BOOLEAN" -> anyValueMatches(
                    criterion.conditionValues(),
                    candidate -> Boolean.parseBoolean(candidate) == asBoolean(cell));
            case "NUMBER_EQ" -> compareNumbers(cell, criterion.conditionValues(), comparisonType.EXACT);
            case "NUMBER_GREATER" -> compareNumbers(cell, criterion.conditionValues(), comparisonType.GREATER_THAN);
            case "NUMBER_GREATER_THAN_EQ" -> compareNumbers(cell, criterion.conditionValues(), comparisonType.GREATER_THAN_OR_EQUAL);
            case "NUMBER_LESS" -> compareNumbers(cell, criterion.conditionValues(), comparisonType.LESS_THAN);
            case "NUMBER_LESS_THAN_EQ" -> compareNumbers(cell, criterion.conditionValues(), comparisonType.LESS_THAN_OR_EQUAL);
            case "NUMBER_BETWEEN" -> compareNumberRange(cell, criterion.conditionValues(), true);
            case "NUMBER_NOT_BETWEEN" -> compareNumberRange(cell, criterion.conditionValues(), false);
            default -> true;
        };
    }

    private static boolean anyValueMatches(List<String> values, java.util.function.Predicate<String> predicate) {
        if (values.isEmpty()) {
            return false;
        }
        return values.stream().filter(Objects::nonNull).anyMatch(predicate);
    }

    private static boolean compareNumbers(CellValue cell, List<String> values, comparisonType comparisonType) {
        BigDecimal rowValue = asNumber(cell);
        if (rowValue == null || values.isEmpty()) {
            return false;
        }

        BigDecimal filterValue = parseBigDecimal(values.getFirst());
        if (filterValue == null) {
            return false;
        }

        int comparison = rowValue.compareTo(filterValue);
        return switch (comparisonType) {
            case EXACT -> comparison == 0;
            case GREATER_THAN -> comparison > 0;
            case GREATER_THAN_OR_EQUAL -> comparison >= 0;
            case LESS_THAN -> comparison < 0;
            case LESS_THAN_OR_EQUAL -> comparison <= 0;
        };
    }

    private static boolean compareNumberRange(CellValue cell, List<String> values, boolean inclusive) {
        BigDecimal rowValue = asNumber(cell);
        if (rowValue == null || values.size() < 2) {
            return false;
        }

        BigDecimal lower = parseBigDecimal(values.get(0));
        BigDecimal upper = parseBigDecimal(values.get(1));
        if (lower == null || upper == null) {
            return false;
        }

        boolean withinRange = rowValue.compareTo(lower) >= 0 && rowValue.compareTo(upper) <= 0;
        return inclusive ? withinRange : !withinRange;
    }

    private static Comparator<WorksheetRow> comparator(FilterViewDefinition filterView) {
        Comparator<WorksheetRow> comparator = Comparator.comparingInt(WorksheetRow::sheetRowIndex);
        for (SortKey sortKey : filterView.sortKeys()) {
            Comparator<WorksheetRow> nextComparator = Comparator.comparing(
                    row -> sortableValue(row, sortKey.columnIndex()),
                    OrriDatabase::compareSortValues);
            if (sortKey.descending()) {
                nextComparator = nextComparator.reversed();
            }
            comparator = nextComparator.thenComparing(comparator);
        }
        return comparator;
    }

    private static CellValue sortableValue(WorksheetRow row, int columnIndex) {
        return columnIndex < row.cells().size() ? row.cells().get(columnIndex) : CellValue.blank();
    }

    private static int compareSortValues(CellValue left, CellValue right) {
        if (left.kind() == ValueKind.BLANK && right.kind() == ValueKind.BLANK) {
            return 0;
        }
        if (left.kind() == ValueKind.BLANK) {
            return 1;
        }
        if (right.kind() == ValueKind.BLANK) {
            return -1;
        }

        BigDecimal leftNumber = asNumber(left);
        BigDecimal rightNumber = asNumber(right);
        if (leftNumber != null && rightNumber != null) {
            return leftNumber.compareTo(rightNumber);
        }

        if (left.kind() == ValueKind.BOOLEAN && right.kind() == ValueKind.BOOLEAN) {
            return Boolean.compare(asBoolean(left), asBoolean(right));
        }

        String leftValue = left.displayValue() == null ? "" : left.displayValue();
        String rightValue = right.displayValue() == null ? "" : right.displayValue();
        return leftValue.compareToIgnoreCase(rightValue);
    }

    private static List<Object> coerceRow(WorksheetRow row, List<ColumnType> columnTypes) {
        List<Object> values = new ArrayList<>(columnTypes.size());
        for (int columnIndex = 0; columnIndex < columnTypes.size(); columnIndex++) {
            CellValue cell = columnIndex < row.cells().size() ? row.cells().get(columnIndex) : CellValue.blank();
            values.add(coerceCell(cell, columnTypes.get(columnIndex)));
        }
        return values;
    }

    private static Object coerceCell(CellValue cell, ColumnType columnType) {
        return switch (columnType) {
            case BOOLEAN -> cell.kind() == ValueKind.BLANK ? null : asBoolean(cell);
            case DECIMAL -> asNumber(cell);
            case VARCHAR -> cell.displayValue();
        };
    }

    private static boolean asBoolean(CellValue cell) {
        if (cell.kind() == ValueKind.BOOLEAN && cell.typedValue() instanceof Boolean value) {
            return value;
        }
        return Boolean.parseBoolean(cell.displayValue());
    }

    private static BigDecimal asNumber(CellValue cell) {
        if (cell.kind() == ValueKind.NUMBER && cell.typedValue() instanceof BigDecimal value) {
            return value;
        }
        return parseBigDecimal(cell.displayValue());
    }

    private static BigDecimal parseBigDecimal(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return new BigDecimal(value.trim());
        } catch (NumberFormatException exception) {
            return null;
        }
    }

    private static void createTable(
            Connection connection,
            String relationName,
            List<String> columnNames,
            List<ColumnType> columnTypes,
            List<List<Object>> rows) throws SQLException {
        if (columnNames.isEmpty()) {
            throw new SQLException("Relation " + relationName + " does not contain any columns");
        }

        String createSql = buildCreateTableSql(relationName, columnNames, columnTypes);
        try (Statement statement = connection.createStatement()) {
            statement.execute(createSql);
        }

        if (rows.isEmpty()) {
            return;
        }

        String insertSql = buildInsertSql(relationName, columnNames);
        try (PreparedStatement preparedStatement = connection.prepareStatement(insertSql)) {
            for (List<Object> row : rows) {
                for (int index = 0; index < row.size(); index++) {
                    preparedStatement.setObject(index + 1, row.get(index));
                }
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
        }
    }

    private static String buildCreateTableSql(String relationName, List<String> columnNames, List<ColumnType> columnTypes) {
        List<String> definitions = new ArrayList<>(columnNames.size());
        for (int index = 0; index < columnNames.size(); index++) {
            definitions.add(quote(columnNames.get(index)) + " " + columnTypes.get(index).ddlType());
        }
        return "CREATE TABLE " + quote(relationName) + " (" + String.join(", ", definitions) + ")";
    }

    private static String buildInsertSql(String relationName, List<String> columnNames) {
        String placeholders = String.join(", ", java.util.Collections.nCopies(columnNames.size(), "?"));
        return "INSERT INTO " + quote(relationName) + " VALUES (" + placeholders + ")";
    }

    private static String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private enum comparisonType {
        EXACT,
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL,
        LESS_THAN,
        LESS_THAN_OR_EQUAL
    }
}
