package io.github.rahulsom.orri;

import java.sql.Types;
import java.util.List;
import java.util.Set;

/**
 * Immutable snapshot of the worksheets and filter views in a spreadsheet.
 */
record SpreadsheetSnapshot(List<WorksheetSnapshot> worksheets, List<FilterViewDefinition> filterViews) {

    Set<String> viewNames() {
        return filterViews.stream()
                .map(FilterViewDefinition::name)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

    WorksheetSnapshot worksheet(String relationName) {
        return worksheets.stream()
                .filter(worksheet -> worksheet.name().equals(relationName))
                .findFirst()
                .orElse(null);
    }

    List<FilterViewDefinition> filterViewsForWorksheet(String worksheetName) {
        WorksheetSnapshot worksheet = worksheet(worksheetName);
        if (worksheet == null) {
            return List.of();
        }

        return filterViews.stream()
                .filter(filterView -> filterView.sourceSheetId() == worksheet.sheetId())
                .toList();
    }
}

/**
 * Immutable representation of one worksheet exposed as a JDBC table.
 */
record WorksheetSnapshot(
        int sheetId, String name, List<String> columnNames, List<ColumnType> columnTypes, List<WorksheetRow> rows) {}

/**
 * One logical worksheet row with its original Google Sheets row index.
 */
record WorksheetRow(int sheetRowIndex, List<CellValue> cells) {}

/**
 * Cell value with both typed and display-oriented representations.
 */
record CellValue(Object typedValue, String displayValue, ValueKind kind) {

    static CellValue blank() {
        return new CellValue(null, null, ValueKind.BLANK);
    }
}

/**
 * Definition of a Google Sheets filter view exposed as a JDBC view.
 */
record FilterViewDefinition(
        Integer filterViewId,
        String name,
        int sourceSheetId,
        int startRowIndex,
        Integer endRowIndex,
        int startColumnIndex,
        Integer endColumnIndex,
        List<FilterCriterion> criteria,
        List<SortKey> sortKeys) {}

/**
 * One filter predicate applied to a filter-view column.
 */
record FilterCriterion(
        int columnIndex, List<String> hiddenValues, String conditionType, List<String> conditionValues) {}

/**
 * Sort instruction applied while materializing a filter view.
 */
record SortKey(int columnIndex, boolean descending) {}

/**
 * Normalized value categories used for filtering and type inference.
 */
enum ValueKind {
    BOOLEAN,
    NUMBER,
    STRING,
    BLANK
}

/**
 * Supported JDBC column types inferred from worksheet contents.
 */
enum ColumnType {
    BOOLEAN("BOOLEAN", Types.BOOLEAN),
    DECIMAL("DECIMAL(38, 10)", Types.DECIMAL),
    VARCHAR("VARCHAR", Types.VARCHAR);

    private final String ddlType;
    private final int sqlType;

    ColumnType(String ddlType, int sqlType) {
        this.ddlType = ddlType;
        this.sqlType = sqlType;
    }

    String ddlType() {
        return ddlType;
    }

    int sqlType() {
        return sqlType;
    }
}
