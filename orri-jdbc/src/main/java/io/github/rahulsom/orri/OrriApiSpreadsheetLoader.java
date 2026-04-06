package io.github.rahulsom.orri;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.BooleanCondition;
import com.google.api.services.sheets.v4.model.CellData;
import com.google.api.services.sheets.v4.model.ConditionValue;
import com.google.api.services.sheets.v4.model.DataValidationRule;
import com.google.api.services.sheets.v4.model.ExtendedValue;
import com.google.api.services.sheets.v4.model.FilterCriteria;
import com.google.api.services.sheets.v4.model.FilterView;
import com.google.api.services.sheets.v4.model.GridData;
import com.google.api.services.sheets.v4.model.GridRange;
import com.google.api.services.sheets.v4.model.NumberFormat;
import com.google.api.services.sheets.v4.model.RowData;
import com.google.api.services.sheets.v4.model.SortSpec;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Loads spreadsheet structure and cells from the Google Sheets API.
 */
final class OrriApiSpreadsheetLoader implements SpreadsheetLoader {
    private static final GsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();

    @Override
    public SpreadsheetSnapshot load(OrriJdbcUrl url) throws SQLException {
        try {
            Sheets sheets = buildSheetsClient(url);
            Sheets.Spreadsheets.Get request = sheets.spreadsheets().get(url.spreadsheetId());
            request.setIncludeGridData(true);

            String apiKey = url.property("apiKey");
            if (apiKey != null && !apiKey.isBlank()) {
                request.setKey(apiKey);
            }

            Spreadsheet spreadsheet = request.execute();
            return toSnapshot(spreadsheet);
        } catch (IOException exception) {
            throw new SQLException("Failed to load spreadsheet " + url.spreadsheetId(), exception);
        }
    }

    private Sheets buildSheetsClient(OrriJdbcUrl url) throws IOException {
        HttpTransport transport = new NetHttpTransport();
        return new Sheets.Builder(transport, JSON_FACTORY, requestInitializer(url))
                .setApplicationName(url.applicationName())
                .build();
    }

    private HttpRequestInitializer requestInitializer(OrriJdbcUrl url) throws IOException {
        String accessToken = url.property("accessToken");
        if (accessToken != null && !accessToken.isBlank()) {
            return request -> authorizeWithBearerToken(request, accessToken);
        }

        String credentialsJson = url.property("credentialsJson");
        if (credentialsJson != null && !credentialsJson.isBlank()) {
            try (InputStream inputStream = new ByteArrayInputStream(credentialsJson.getBytes(StandardCharsets.UTF_8))) {
                GoogleCredentials credentials = GoogleCredentials.fromStream(inputStream)
                        .createScoped(SheetsScopes.SPREADSHEETS_READONLY);
                return new HttpCredentialsAdapter(credentials);
            }
        }

        String credentialsFile = url.property("credentialsFile");
        if (credentialsFile != null && !credentialsFile.isBlank()) {
            try (InputStream inputStream = java.nio.file.Files.newInputStream(java.nio.file.Path.of(credentialsFile))) {
                GoogleCredentials credentials = GoogleCredentials.fromStream(inputStream)
                        .createScoped(SheetsScopes.SPREADSHEETS_READONLY);
                return new HttpCredentialsAdapter(credentials);
            }
        }

        String apiKey = url.property("apiKey");
        if (apiKey != null && !apiKey.isBlank()) {
            return request -> {
            };
        }

        GoogleCredentials credentials = GoogleCredentials.getApplicationDefault()
                .createScoped(SheetsScopes.SPREADSHEETS_READONLY);
        return new HttpCredentialsAdapter(credentials);
    }

    private void authorizeWithBearerToken(HttpRequest request, String accessToken) {
        request.getHeaders().setAuthorization("Bearer " + accessToken);
    }

    private SpreadsheetSnapshot toSnapshot(Spreadsheet spreadsheet) throws SQLException {
        List<WorksheetSnapshot> worksheets = new ArrayList<>();
        Map<Integer, WorksheetSnapshot> worksheetsById = new LinkedHashMap<>();
        List<FilterViewDefinition> filterViews = new ArrayList<>();

        for (com.google.api.services.sheets.v4.model.Sheet sheet : spreadsheet.getSheets()) {
            WorksheetSnapshot worksheet = toWorksheet(sheet);
            worksheets.add(worksheet);
            worksheetsById.put(sheet.getProperties().getSheetId(), worksheet);
        }

        for (com.google.api.services.sheets.v4.model.Sheet sheet : spreadsheet.getSheets()) {
            List<FilterView> sheetFilterViews = sheet.getFilterViews();
            if (sheetFilterViews == null) {
                continue;
            }

            for (FilterView filterView : sheetFilterViews) {
                FilterViewDefinition filterViewDefinition = toFilterView(filterView, worksheetsById);
                if (filterViewDefinition != null) {
                    filterViews.add(filterViewDefinition);
                }
            }
        }

        return new SpreadsheetSnapshot(List.copyOf(worksheets), List.copyOf(filterViews));
    }

    private WorksheetSnapshot toWorksheet(com.google.api.services.sheets.v4.model.Sheet sheet) throws SQLException {
        GridData gridData = first(sheet.getData());
        List<RowData> rowData = gridData == null || gridData.getRowData() == null
                ? List.of()
                : gridData.getRowData();
        if (rowData.isEmpty()) {
            throw new SQLException("Sheet " + sheet.getProperties().getTitle() + " does not contain a header row");
        }

        int columnCount = rowData.stream()
                .map(RowData::getValues)
                .filter(Objects::nonNull)
                .mapToInt(List::size)
                .max()
                .orElse(0);
        if (columnCount == 0) {
            throw new SQLException("Sheet " + sheet.getProperties().getTitle() + " does not contain any columns");
        }

        List<CellData> headerCells = rowData.getFirst().getValues() == null
                ? List.of()
                : rowData.getFirst().getValues();
        List<String> headers = sanitizeHeaders(headerCells, columnCount);

        List<WorksheetRow> rows = new ArrayList<>();
        for (int rowIndex = 1; rowIndex < rowData.size(); rowIndex++) {
            RowData currentRow = rowData.get(rowIndex);
            List<CellValue> cells = new ArrayList<>(columnCount);
            boolean hasValues = false;
            List<CellData> currentCells = currentRow.getValues() == null ? List.of() : currentRow.getValues();

            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                CellValue cellValue = columnIndex < currentCells.size()
                        ? toCellValue(currentCells.get(columnIndex))
                        : CellValue.blank();
                cells.add(cellValue);
                hasValues = hasValues || cellValue.kind() != ValueKind.BLANK;
            }

            if (hasValues) {
                rows.add(new WorksheetRow(rowIndex, List.copyOf(cells)));
            }
        }

        List<ColumnType> columnTypes = inferColumnTypes(rows, columnCount);
        return new WorksheetSnapshot(
                sheet.getProperties().getSheetId(),
                sheet.getProperties().getTitle(),
                List.copyOf(headers),
                List.copyOf(columnTypes),
                List.copyOf(rows));
    }

    private FilterViewDefinition toFilterView(
            FilterView filterView,
            Map<Integer, WorksheetSnapshot> worksheetsById) throws SQLException {
        GridRange range = filterView.getRange();
        if (range == null || range.getSheetId() == null) {
            return null;
        }

        WorksheetSnapshot worksheet = worksheetsById.get(range.getSheetId());
        if (worksheet == null) {
            throw new SQLException("Filter view " + filterView.getTitle() + " points at a missing sheet");
        }

        List<FilterCriterion> criteria = new ArrayList<>();
        Map<String, FilterCriteria> criteriaMap = filterView.getCriteria();
        if (criteriaMap != null) {
            for (Map.Entry<String, FilterCriteria> entry : criteriaMap.entrySet()) {
                FilterCriteria value = entry.getValue();
                criteria.add(new FilterCriterion(
                        Integer.parseInt(entry.getKey()),
                        value.getHiddenValues() == null ? List.of() : List.copyOf(value.getHiddenValues()),
                        value.getCondition() == null ? null : value.getCondition().getType(),
                        conditionValues(value.getCondition())));
            }
        }

        List<SortKey> sortKeys = new ArrayList<>();
        if (filterView.getSortSpecs() != null) {
            for (SortSpec sortSpec : filterView.getSortSpecs()) {
                sortKeys.add(new SortKey(
                        sortSpec.getDimensionIndex(),
                        "DESCENDING".equalsIgnoreCase(sortSpec.getSortOrder())));
            }
        }

        return new FilterViewDefinition(
                filterView.getFilterViewId(),
                filterView.getTitle() == null || filterView.getTitle().isBlank()
                        ? "filter_view_" + filterView.getFilterViewId()
                        : filterView.getTitle(),
                worksheet.sheetId(),
                defaultValue(range.getStartRowIndex(), 0),
                range.getEndRowIndex(),
                defaultValue(range.getStartColumnIndex(), 0),
                range.getEndColumnIndex(),
                List.copyOf(criteria),
                List.copyOf(sortKeys));
    }

    private List<String> conditionValues(BooleanCondition condition) {
        if (condition == null || condition.getValues() == null) {
            return List.of();
        }

        return condition.getValues().stream()
                .map(ConditionValue::getUserEnteredValue)
                .filter(Objects::nonNull)
                .toList();
    }

    private List<String> sanitizeHeaders(List<CellData> headerCells, int columnCount) {
        Map<String, Integer> seen = new LinkedHashMap<>();
        List<String> headers = new ArrayList<>(columnCount);
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            String rawHeader = columnIndex < headerCells.size()
                    ? normalizeHeader(headerCells.get(columnIndex).getFormattedValue())
                    : null;
            String baseName = rawHeader == null || rawHeader.isBlank()
                    ? "column_" + (columnIndex + 1)
                    : rawHeader;
            int count = seen.merge(baseName.toLowerCase(Locale.ROOT), 1, Integer::sum);
            headers.add(count == 1 ? baseName : baseName + "_" + count);
        }
        return headers;
    }

    private String normalizeHeader(String header) {
        return header == null ? null : header.trim();
    }

    private List<ColumnType> inferColumnTypes(List<WorksheetRow> rows, int columnCount) {
        List<ColumnType> columnTypes = new ArrayList<>(columnCount);
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            boolean sawValue = false;
            boolean allBoolean = true;
            boolean allNumbers = true;

            for (WorksheetRow row : rows) {
                CellValue cell = row.cells().get(columnIndex);
                if (cell.kind() == ValueKind.BLANK) {
                    continue;
                }

                sawValue = true;
                allBoolean &= isBooleanLike(cell);
                allNumbers &= cell.kind() == ValueKind.NUMBER;
            }

            if (!sawValue) {
                columnTypes.add(ColumnType.VARCHAR);
            } else if (allBoolean) {
                columnTypes.add(ColumnType.BOOLEAN);
            } else if (allNumbers) {
                columnTypes.add(ColumnType.DECIMAL);
            } else {
                columnTypes.add(ColumnType.VARCHAR);
            }
        }
        return columnTypes;
    }

    private boolean isBooleanLike(CellValue cellValue) {
        if (cellValue.kind() == ValueKind.BOOLEAN) {
            return true;
        }

        if (cellValue.kind() != ValueKind.STRING || cellValue.displayValue() == null) {
            return false;
        }

        String normalized = cellValue.displayValue().trim();
        return "true".equalsIgnoreCase(normalized) || "false".equalsIgnoreCase(normalized);
    }

    private CellValue toCellValue(CellData cellData) {
        if (cellData == null) {
            return CellValue.blank();
        }

        String formattedValue = cellData.getFormattedValue();
        ExtendedValue effectiveValue = cellData.getEffectiveValue();
        if (isRenderedAsString(cellData) && formattedValue != null && !formattedValue.isBlank()) {
            return new CellValue(formattedValue, formattedValue, ValueKind.STRING);
        }

        if (effectiveValue != null) {
            if (effectiveValue.getBoolValue() != null) {
                Boolean value = effectiveValue.getBoolValue();
                return new CellValue(value, String.valueOf(value), ValueKind.BOOLEAN);
            }

            if (effectiveValue.getNumberValue() != null) {
                BigDecimal value = BigDecimal.valueOf(effectiveValue.getNumberValue());
                return new CellValue(value, formattedValue == null ? value.toPlainString() : formattedValue, ValueKind.NUMBER);
            }

            if (effectiveValue.getStringValue() != null) {
                String value = effectiveValue.getStringValue();
                return new CellValue(value, value, ValueKind.STRING);
            }
        }

        if (formattedValue != null && !formattedValue.isBlank()) {
            if (isCheckboxCell(cellData) && ("true".equalsIgnoreCase(formattedValue) || "false".equalsIgnoreCase(formattedValue))) {
                Boolean value = Boolean.parseBoolean(formattedValue);
                return new CellValue(value, formattedValue, ValueKind.BOOLEAN);
            }
            return new CellValue(formattedValue, formattedValue, ValueKind.STRING);
        }

        return CellValue.blank();
    }

    private boolean isRenderedAsString(CellData cellData) {
        NumberFormat numberFormat = cellData.getEffectiveFormat() == null
                ? null
                : cellData.getEffectiveFormat().getNumberFormat();
        if (numberFormat == null || numberFormat.getType() == null) {
            return false;
        }

        return switch (numberFormat.getType()) {
            case "DATE", "DATE_TIME", "TIME", "TIME_DURATION" -> true;
            default -> false;
        };
    }

    private boolean isCheckboxCell(CellData cellData) {
        DataValidationRule validationRule = cellData.getDataValidation();
        if (validationRule == null || validationRule.getCondition() == null) {
            return false;
        }
        return "BOOLEAN".equalsIgnoreCase(validationRule.getCondition().getType());
    }

    private <T> T first(List<T> values) {
        return values == null || values.isEmpty() ? null : values.getFirst();
    }

    private int defaultValue(Integer value, int fallback) {
        return value == null ? fallback : value;
    }
}
