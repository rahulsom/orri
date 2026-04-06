package io.github.rahulsom.orri;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.AddFilterViewRequest;
import com.google.api.services.sheets.v4.model.AddSheetRequest;
import com.google.api.services.sheets.v4.model.BatchUpdateSpreadsheetRequest;
import com.google.api.services.sheets.v4.model.BatchUpdateSpreadsheetResponse;
import com.google.api.services.sheets.v4.model.BooleanCondition;
import com.google.api.services.sheets.v4.model.ClearValuesRequest;
import com.google.api.services.sheets.v4.model.ConditionValue;
import com.google.api.services.sheets.v4.model.DeleteFilterViewRequest;
import com.google.api.services.sheets.v4.model.DeleteSheetRequest;
import com.google.api.services.sheets.v4.model.FilterCriteria;
import com.google.api.services.sheets.v4.model.FilterView;
import com.google.api.services.sheets.v4.model.GridRange;
import com.google.api.services.sheets.v4.model.Request;
import com.google.api.services.sheets.v4.model.SheetProperties;
import com.google.api.services.sheets.v4.model.SortSpec;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Synchronizes worksheet contents to Google Sheets using the Sheets API.
 */
final class OrriApiSpreadsheetSynchronizer implements SpreadsheetSynchronizer {
    private static final GsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();

    @Override
    public void syncWorksheet(OrriJdbcUrl url, WorksheetSnapshot worksheet, Connection connection) throws SQLException {
        try {
            Sheets sheets = buildSheetsClient(url);
            String sheetRange = quoteSheetName(worksheet.name());

            sheets.spreadsheets()
                    .values()
                    .clear(url.spreadsheetId(), sheetRange, new ClearValuesRequest())
                    .execute();

            List<List<Object>> values = loadWorksheetValues(connection, worksheet);
            ValueRange valueRange = new ValueRange().setValues(values);
            sheets.spreadsheets()
                    .values()
                    .update(url.spreadsheetId(), sheetRange + "!A1", valueRange)
                    .setValueInputOption("USER_ENTERED")
                    .execute();
        } catch (IOException exception) {
            throw new SQLException("Failed to synchronize worksheet " + worksheet.name(), exception);
        }
    }

    @Override
    public WorksheetSnapshot createWorksheet(OrriJdbcUrl url, WorksheetSnapshot worksheet, Connection connection) throws SQLException {
        try {
            Sheets sheets = buildSheetsClient(url);
            Request request = new Request()
                    .setAddSheet(new AddSheetRequest().setProperties(new SheetProperties().setTitle(worksheet.name())));
            BatchUpdateSpreadsheetResponse response = sheets.spreadsheets()
                    .batchUpdate(
                            url.spreadsheetId(),
                            new BatchUpdateSpreadsheetRequest().setRequests(List.of(request)))
                    .execute();

            Integer sheetId = response.getReplies().getFirst().getAddSheet().getProperties().getSheetId();
            WorksheetSnapshot createdWorksheet = OrriDatabase.readWorksheetSnapshot(connection, worksheet.name(), sheetId);
            syncWorksheet(url, createdWorksheet, connection);
            return createdWorksheet;
        } catch (IOException exception) {
            throw new SQLException("Failed to create worksheet " + worksheet.name(), exception);
        }
    }

    @Override
    public FilterViewDefinition createFilterView(OrriJdbcUrl url, FilterViewDefinition filterView) throws SQLException {
        try {
            Sheets sheets = buildSheetsClient(url);
            FilterView googleFilterView = new FilterView()
                    .setTitle(filterView.name())
                    .setRange(new GridRange()
                            .setSheetId(filterView.sourceSheetId())
                            .setStartRowIndex(filterView.startRowIndex())
                            .setEndRowIndex(filterView.endRowIndex())
                            .setStartColumnIndex(filterView.startColumnIndex())
                            .setEndColumnIndex(filterView.endColumnIndex()))
                    .setCriteria(toCriteria(filterView.criteria()))
                    .setSortSpecs(toSortSpecs(filterView.sortKeys()));

            Request request = new Request()
                    .setAddFilterView(new AddFilterViewRequest().setFilter(googleFilterView));
            BatchUpdateSpreadsheetResponse response = sheets.spreadsheets()
                    .batchUpdate(
                            url.spreadsheetId(),
                            new BatchUpdateSpreadsheetRequest().setRequests(List.of(request)))
                    .execute();
            Integer filterViewId = response.getReplies().getFirst().getAddFilterView().getFilter().getFilterViewId();
            return new FilterViewDefinition(
                    filterViewId,
                    filterView.name(),
                    filterView.sourceSheetId(),
                    filterView.startRowIndex(),
                    filterView.endRowIndex(),
                    filterView.startColumnIndex(),
                    filterView.endColumnIndex(),
                    filterView.criteria(),
                    filterView.sortKeys());
        } catch (IOException exception) {
            throw new SQLException("Failed to create filter view " + filterView.name(), exception);
        }
    }

    @Override
    public void deleteWorksheet(OrriJdbcUrl url, WorksheetSnapshot worksheet) throws SQLException {
        try {
            Sheets sheets = buildSheetsClient(url);
            Request request = new Request()
                    .setDeleteSheet(new DeleteSheetRequest().setSheetId(worksheet.sheetId()));
            sheets.spreadsheets()
                    .batchUpdate(
                            url.spreadsheetId(),
                            new BatchUpdateSpreadsheetRequest().setRequests(List.of(request)))
                    .execute();
        } catch (IOException exception) {
            throw new SQLException("Failed to delete worksheet " + worksheet.name(), exception);
        }
    }

    @Override
    public void deleteFilterView(OrriJdbcUrl url, FilterViewDefinition filterView) throws SQLException {
        if (filterView.filterViewId() == null) {
            throw new SQLException("Cannot delete filter view " + filterView.name() + " without a filter view id");
        }
        try {
            Sheets sheets = buildSheetsClient(url);
            Request request = new Request()
                    .setDeleteFilterView(new DeleteFilterViewRequest().setFilterId(filterView.filterViewId()));
            sheets.spreadsheets()
                    .batchUpdate(
                            url.spreadsheetId(),
                            new BatchUpdateSpreadsheetRequest().setRequests(List.of(request)))
                    .execute();
        } catch (IOException exception) {
            throw new SQLException("Failed to delete filter view " + filterView.name(), exception);
        }
    }

    private Sheets buildSheetsClient(OrriJdbcUrl url) throws IOException, SQLException {
        HttpTransport transport = new NetHttpTransport();
        return new Sheets.Builder(transport, JSON_FACTORY, requestInitializer(url))
                .setApplicationName(url.applicationName())
                .build();
    }

    private HttpRequestInitializer requestInitializer(OrriJdbcUrl url) throws IOException, SQLException {
        String accessToken = url.property("accessToken");
        if (accessToken != null && !accessToken.isBlank()) {
            return request -> authorizeWithBearerToken(request, accessToken);
        }

        String credentialsJson = url.property("credentialsJson");
        if (credentialsJson != null && !credentialsJson.isBlank()) {
            try (InputStream inputStream = new ByteArrayInputStream(credentialsJson.getBytes(StandardCharsets.UTF_8))) {
                GoogleCredentials credentials = GoogleCredentials.fromStream(inputStream)
                        .createScoped(SheetsScopes.SPREADSHEETS);
                return new HttpCredentialsAdapter(credentials);
            }
        }

        String credentialsFile = url.property("credentialsFile");
        if (credentialsFile != null && !credentialsFile.isBlank()) {
            try (InputStream inputStream = java.nio.file.Files.newInputStream(java.nio.file.Path.of(credentialsFile))) {
                GoogleCredentials credentials = GoogleCredentials.fromStream(inputStream)
                        .createScoped(SheetsScopes.SPREADSHEETS);
                return new HttpCredentialsAdapter(credentials);
            }
        }

        if (url.property("apiKey") != null && !url.property("apiKey").isBlank()) {
            throw new SQLException("Writable connections require OAuth credentials or a service account");
        }

        GoogleCredentials credentials = GoogleCredentials.getApplicationDefault()
                .createScoped(SheetsScopes.SPREADSHEETS);
        return new HttpCredentialsAdapter(credentials);
    }

    private void authorizeWithBearerToken(HttpRequest request, String accessToken) {
        request.getHeaders().setAuthorization("Bearer " + accessToken);
    }

    private List<List<Object>> loadWorksheetValues(Connection connection, WorksheetSnapshot worksheet) throws SQLException {
        List<List<Object>> values = new ArrayList<>();
        values.add(new ArrayList<>(worksheet.columnNames()));

        String sql = "select " + selectColumns(worksheet.columnNames()) + " from " + quoteIdentifier(worksheet.name());
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                List<Object> row = new ArrayList<>(worksheet.columnNames().size());
                for (int columnIndex = 0; columnIndex < worksheet.columnNames().size(); columnIndex++) {
                    Object value = resultSet.getObject(columnIndex + 1);
                    row.add(toCellValue(value));
                }
                values.add(row);
            }
        }

        return values;
    }

    private String selectColumns(List<String> columnNames) {
        return columnNames.stream()
                .map(OrriApiSpreadsheetSynchronizer::quoteIdentifier)
                .collect(java.util.stream.Collectors.joining(", "));
    }

    private Object toCellValue(Object value) {
        if (value == null) {
            return "";
        }
        if (value instanceof BigDecimal bigDecimal) {
            return bigDecimal.toPlainString();
        }
        return value;
    }

    private static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private java.util.Map<String, FilterCriteria> toCriteria(List<FilterCriterion> criteria) {
        java.util.Map<String, FilterCriteria> result = new java.util.LinkedHashMap<>();
        for (FilterCriterion criterion : criteria) {
            FilterCriteria filterCriteria = new FilterCriteria();
            if (!criterion.hiddenValues().isEmpty()) {
                filterCriteria.setHiddenValues(criterion.hiddenValues());
            }
            if (criterion.conditionType() != null) {
                filterCriteria.setCondition(new BooleanCondition()
                        .setType(criterion.conditionType())
                        .setValues(criterion.conditionValues().stream()
                                .map(value -> new ConditionValue().setUserEnteredValue(value))
                                .toList()));
            }
            result.put(String.valueOf(criterion.columnIndex()), filterCriteria);
        }
        return result;
    }

    private List<SortSpec> toSortSpecs(List<SortKey> sortKeys) {
        return sortKeys.stream()
                .map(sortKey -> new SortSpec()
                        .setDimensionIndex(sortKey.columnIndex())
                        .setSortOrder(sortKey.descending() ? "DESCENDING" : "ASCENDING"))
                .toList();
    }

    private String quoteSheetName(String sheetName) {
        return "'" + sheetName.replace("'", "''") + "'";
    }
}
