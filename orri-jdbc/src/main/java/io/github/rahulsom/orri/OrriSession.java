package io.github.rahulsom.orri;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Mutable per-connection session state for Orri connections.
 */
final class OrriSession {
    private final Connection connection;
    private final OrriJdbcUrl url;
    private final SpreadsheetSynchronizer synchronizer;
    private final boolean readOnly;
    private SpreadsheetSnapshot snapshot;

    OrriSession(
            Connection connection,
            OrriJdbcUrl url,
            SpreadsheetSnapshot snapshot,
            SpreadsheetSynchronizer synchronizer,
            boolean readOnly) {
        this.connection = connection;
        this.url = url;
        this.snapshot = snapshot;
        this.synchronizer = synchronizer;
        this.readOnly = readOnly;
    }

    Connection connection() {
        return connection;
    }

    OrriJdbcUrl url() {
        return url;
    }

    SpreadsheetSynchronizer synchronizer() {
        return synchronizer;
    }

    boolean readOnly() {
        return readOnly;
    }

    synchronized SpreadsheetSnapshot snapshot() {
        return snapshot;
    }

    synchronized void registerWorksheet(WorksheetSnapshot worksheet) {
        List<WorksheetSnapshot> worksheets = new ArrayList<>(snapshot.worksheets());
        worksheets.add(worksheet);
        snapshot = new SpreadsheetSnapshot(List.copyOf(worksheets), snapshot.filterViews());
    }

    synchronized void replaceWorksheet(String worksheetName, WorksheetSnapshot worksheet) {
        List<WorksheetSnapshot> worksheets = snapshot.worksheets().stream()
                .filter(currentWorksheet -> !currentWorksheet.name().equals(worksheetName))
                .collect(java.util.stream.Collectors.toCollection(ArrayList::new));
        worksheets.add(worksheet);
        snapshot = new SpreadsheetSnapshot(List.copyOf(worksheets), snapshot.filterViews());
    }

    synchronized void registerFilterView(FilterViewDefinition filterView) {
        List<FilterViewDefinition> filterViews = new ArrayList<>(snapshot.filterViews());
        filterViews.add(filterView);
        snapshot = new SpreadsheetSnapshot(snapshot.worksheets(), List.copyOf(filterViews));
    }

    synchronized void replaceFilterView(String viewName, FilterViewDefinition filterView) {
        List<FilterViewDefinition> filterViews = snapshot.filterViews().stream()
                .filter(currentFilterView -> !currentFilterView.name().equals(viewName))
                .collect(java.util.stream.Collectors.toCollection(ArrayList::new));
        filterViews.add(filterView);
        snapshot = new SpreadsheetSnapshot(snapshot.worksheets(), List.copyOf(filterViews));
    }

    synchronized void unregisterWorksheet(String worksheetName) {
        List<WorksheetSnapshot> worksheets = snapshot.worksheets().stream()
                .filter(worksheet -> !worksheet.name().equals(worksheetName))
                .toList();
        List<FilterViewDefinition> filterViews = snapshot.filterViews().stream()
                .filter(filterView -> {
                    WorksheetSnapshot worksheet = snapshot.worksheet(worksheetName);
                    return worksheet == null || filterView.sourceSheetId() != worksheet.sheetId();
                })
                .toList();
        snapshot = new SpreadsheetSnapshot(List.copyOf(worksheets), List.copyOf(filterViews));
    }

    synchronized void unregisterFilterView(String viewName) {
        List<FilterViewDefinition> filterViews = snapshot.filterViews().stream()
                .filter(filterView -> !filterView.name().equals(viewName))
                .toList();
        snapshot = new SpreadsheetSnapshot(snapshot.worksheets(), List.copyOf(filterViews));
    }

    synchronized FilterViewDefinition filterView(String viewName) {
        return snapshot.filterViews().stream()
                .filter(filterView -> filterView.name().equals(viewName))
                .findFirst()
                .orElse(null);
    }

    synchronized void syncWorksheet(String worksheetName) throws SQLException {
        WorksheetSnapshot worksheet = snapshot.worksheet(worksheetName);
        if (worksheet == null) {
            worksheet = OrriDatabase.readWorksheetSnapshot(connection, worksheetName, null);
        }
        synchronizer.syncWorksheet(url, worksheet, connection);
        OrriDatabase.refreshFilterViews(connection, snapshot, worksheetName);
    }
}
