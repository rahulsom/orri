package io.github.rahulsom.orri;

import java.sql.SQLException;

/**
 * Loads an immutable spreadsheet snapshot for a JDBC connection request.
 */
interface SpreadsheetLoader {
    SpreadsheetSnapshot load(OrriJdbcUrl url) throws SQLException;
}
