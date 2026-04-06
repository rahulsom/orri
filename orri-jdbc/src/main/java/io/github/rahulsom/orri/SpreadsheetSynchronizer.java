package io.github.rahulsom.orri;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Writes worksheet changes from the JDBC session back to Google Sheets.
 */
interface SpreadsheetSynchronizer {
    void syncWorksheet(OrriJdbcUrl url, WorksheetSnapshot worksheet, Connection connection) throws SQLException;

    WorksheetSnapshot createWorksheet(OrriJdbcUrl url, WorksheetSnapshot worksheet, Connection connection)
            throws SQLException;

    FilterViewDefinition createFilterView(OrriJdbcUrl url, FilterViewDefinition filterView) throws SQLException;

    void deleteWorksheet(OrriJdbcUrl url, WorksheetSnapshot worksheet) throws SQLException;

    void deleteFilterView(OrriJdbcUrl url, FilterViewDefinition filterView) throws SQLException;

    WorksheetSnapshot renameWorksheet(OrriJdbcUrl url, WorksheetSnapshot worksheet, String newName) throws SQLException;

    FilterViewDefinition updateFilterView(
            OrriJdbcUrl url, FilterViewDefinition existingFilterView, FilterViewDefinition updatedFilterView)
            throws SQLException;
}
