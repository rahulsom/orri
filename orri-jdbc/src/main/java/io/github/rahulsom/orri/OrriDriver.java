package io.github.rahulsom.orri;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * JDBC driver that exposes a Google Sheets spreadsheet as a JDBC database.
 */
public final class OrriDriver implements Driver {
    static {
        try {
            DriverManager.registerDriver(new OrriDriver());
        } catch (SQLException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }

    private final SpreadsheetLoader loader;
    private final SpreadsheetSynchronizer synchronizer;

    /**
     * Creates a driver backed by the Google Sheets API.
     */
    public OrriDriver() {
        this(new OrriApiSpreadsheetLoader(), new OrriApiSpreadsheetSynchronizer());
    }

    OrriDriver(SpreadsheetLoader loader, SpreadsheetSynchronizer synchronizer) {
        this.loader = Objects.requireNonNull(loader);
        this.synchronizer = Objects.requireNonNull(synchronizer);
    }

    /**
     * Connects to the spreadsheet identified by the supplied Orri JDBC URL.
     *
     * @param url the JDBC URL
     * @param info optional connection properties
     * @return a JDBC connection, or {@code null} when the URL is not supported
     * @throws SQLException if the spreadsheet cannot be loaded
     */
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        OrriJdbcUrl jdbcUrl = OrriJdbcUrl.parse(url, info);
        boolean readOnly = jdbcUrl.readOnly();
        SpreadsheetSnapshot snapshot = loader.load(jdbcUrl);
        Connection connection = OrriDatabase.materialize(snapshot, readOnly);
        OrriSession session = new OrriSession(connection, jdbcUrl, snapshot, synchronizer, readOnly);
        return OrriJdbcProxy.wrap(session);
    }

    /**
     * Returns whether the supplied URL uses the Orri JDBC scheme.
     *
     * @param url the candidate JDBC URL
     * @return {@code true} when the URL is supported by this driver
     */
    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(OrriJdbcUrl.PREFIX);
    }

    /**
     * Describes the optional connection properties accepted by this driver.
     *
     * @param url the JDBC URL
     * @param info existing connection properties
     * @return the supported driver properties
     */
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[]{
                property("apiKey", "Google API key for public spreadsheets"),
                property("credentialsFile", "Path to a Google service account JSON file"),
                property("credentialsJson", "Inline Google service account JSON"),
                property("accessToken", "OAuth access token for the Sheets API"),
                property("readOnly", "Whether the connection should remain read-only (default: false)"),
                property("applicationName", "Application name sent to the Google Sheets API")
        };
    }

    private DriverPropertyInfo property(String name, String description) {
        DriverPropertyInfo propertyInfo = new DriverPropertyInfo(name, null);
        propertyInfo.description = description;
        return propertyInfo;
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    /**
     * Returns the parent logger for the driver.
     *
     * @return the driver logger
     */
    @Override
    public Logger getParentLogger() {
        return Logger.getLogger("com.github.rahulsom.orri");
    }
}
