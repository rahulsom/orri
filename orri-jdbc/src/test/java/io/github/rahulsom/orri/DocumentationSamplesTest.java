package io.github.rahulsom.orri;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class DocumentationSamplesTest {
    // tag::jdbc-url[]
    private static final String SPREADSHEET_URL = "jdbc:orri:1yRm-oPjuUGvfy4uhGc-woytgXQ7nSpxalWVX9djruSQ";
    // end::jdbc-url[]

    @Test
    void jdbcUrlUsesTheSpreadsheetId() {
        assertTrue(SPREADSHEET_URL.startsWith("jdbc:orri:"));
    }

    @Test
    void connectWithApiKey() throws Exception {
        try (Connection connection = connectWithApiKey(sampleDriver())) {
            assertNotNull(connection);
            assertTrue(connection.isReadOnly());
            assertEquals(3, rowCount(connection, "Employees"));
        }
    }

    private static Connection connectWithApiKey(OrriDriver driver) throws Exception {
        // tag::connect-api-key[]
        Properties properties = new Properties();
        properties.setProperty("apiKey", "your-google-api-key");
        properties.setProperty("readOnly", "true");

        return driver.connect(SPREADSHEET_URL, properties);
        // end::connect-api-key[]
    }

    @Test
    void connectWithServiceAccount() throws Exception {
        try (Connection connection = connectWithServiceAccount(sampleDriver())) {
            assertNotNull(connection);
            assertFalse(connection.isReadOnly());
            assertEquals(3, rowCount(connection, "Employees"));
        }
    }

    private static Connection connectWithServiceAccount(OrriDriver driver) throws Exception {
        // tag::connect-service-account[]
        Properties properties = new Properties();
        properties.setProperty("credentialsFile", "/path/to/service-account.json");

        return driver.connect(SPREADSHEET_URL, properties);
        // end::connect-service-account[]
    }

    @Test
    void queryWorksheetAndFilterView() throws Exception {
        List<String> rows = queryWorksheetAndFilterView(sampleDriver());

        assertEquals(List.of("Alice -> true", "Bob -> false", "Carol -> true", "Alice", "Carol"), rows);
    }

    private static List<String> queryWorksheetAndFilterView(OrriDriver driver) throws Exception {
        // tag::query-sheet-and-view[]
        Properties properties = new Properties();
        properties.setProperty("apiKey", "your-google-api-key");
        properties.setProperty("readOnly", "true");

        List<String> output = new ArrayList<>();
        try (Connection connection = driver.connect(SPREADSHEET_URL, properties);
                Statement statement = connection.createStatement()) {
            try (ResultSet worksheetRows =
                    statement.executeQuery("select \"Name\", \"Active\" from \"Employees\" order by \"Name\"")) {
                while (worksheetRows.next()) {
                    String name = worksheetRows.getString("Name");
                    boolean active = worksheetRows.getBoolean("Active");
                    output.add(name + " -> " + active);
                }
            }

            try (ResultSet filterViewRows =
                    statement.executeQuery("select \"Name\" from \"Active Employees\" order by \"Name\"")) {
                while (filterViewRows.next()) {
                    output.add(filterViewRows.getString("Name"));
                }
            }
        }
        return output;
        // end::query-sheet-and-view[]
    }

    @Test
    void inspectSchema() throws Exception {
        SchemaSnapshot schemaSnapshot = inspectSchema(sampleDriver());

        assertEquals(List.of("Active Employees (VIEW)", "Employees (TABLE)"), schemaSnapshot.relations());
        assertEquals(List.of("Name VARCHAR", "Active BOOLEAN", "Points DECIMAL"), schemaSnapshot.columns());
    }

    private static SchemaSnapshot inspectSchema(OrriDriver driver) throws Exception {
        // tag::inspect-schema[]
        Properties properties = new Properties();
        properties.setProperty("apiKey", "your-google-api-key");
        properties.setProperty("readOnly", "true");

        List<String> relations = new ArrayList<>();
        List<String> columns = new ArrayList<>();
        try (Connection connection = driver.connect(SPREADSHEET_URL, properties)) {
            DatabaseMetaData metaData = connection.getMetaData();

            try (ResultSet tables = metaData.getTables(null, null, "%", null)) {
                while (tables.next()) {
                    String schemaName = tables.getString("TABLE_SCHEM");
                    String tableName = tables.getString("TABLE_NAME");
                    String tableType = tables.getString("TABLE_TYPE");
                    if (!"INFORMATION_SCHEMA".equalsIgnoreCase(schemaName)) {
                        relations.add(tableName + " (" + normalizeTableType(tableType) + ")");
                    }
                }
            }

            try (ResultSet resultSet = metaData.getColumns(null, null, "Employees", "%")) {
                while (resultSet.next()) {
                    String columnName = resultSet.getString("COLUMN_NAME");
                    String typeName = resultSet.getString("TYPE_NAME");
                    columns.add(columnName + " " + normalizeTypeName(typeName));
                }
            }
        }
        return new SchemaSnapshot(relations, columns);
        // end::inspect-schema[]
    }

    @Test
    void mutateWorksheet() throws Exception {
        List<String> names = mutateWorksheet(sampleDriver());

        assertEquals(List.of("Alice", "Bob", "Carol"), names);
    }

    private static List<String> mutateWorksheet(OrriDriver driver) throws Exception {
        // tag::write-sheet[]
        Properties properties = new Properties();
        properties.setProperty("accessToken", "your-oauth-access-token");

        try (Connection connection = driver.connect(SPREADSHEET_URL, properties);
                Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    "insert into \"Employees\" (\"Name\", \"Active\", \"Points\") values ('Dana', true, 9.5)");
            statement.executeUpdate("update \"Employees\" set \"Active\" = false where \"Name\" = 'Dana'");
            statement.executeUpdate("delete from \"Employees\" where \"Name\" = 'Dana'");

            List<String> names = new ArrayList<>();
            try (ResultSet resultSet = statement.executeQuery("select \"Name\" from \"Employees\" order by \"Name\"")) {
                while (resultSet.next()) {
                    names.add(resultSet.getString("Name"));
                }
            }
            return names;
        }
        // end::write-sheet[]
    }

    @Test
    void createWorksheetAndFilterView() throws Exception {
        SchemaManagementSnapshot snapshot = createWorksheetAndFilterView(sampleDriver());

        assertEquals(List.of("Projects"), snapshot.createdTables());
        assertEquals(List.of("Active Projects"), snapshot.createdViews());
    }

    private static SchemaManagementSnapshot createWorksheetAndFilterView(OrriDriver driver) throws Exception {
        // tag::create-schema[]
        Properties properties = new Properties();
        properties.setProperty("accessToken", "your-oauth-access-token");

        List<String> createdTables = new ArrayList<>();
        List<String> createdViews = new ArrayList<>();
        try (Connection connection = driver.connect(SPREADSHEET_URL, properties);
                Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table "Projects" (
                        "Name" varchar,
                        "Active" boolean,
                        "Budget" decimal(10, 2)
                    )
                    """);
            createdTables.add("Projects");

            statement.execute("""
                    create view "Active Projects" as
                    select "Name", "Active"
                    from "Employees"
                    where "Active" = true
                    order by "Name"
                    """);
            createdViews.add("Active Projects");
        }
        return new SchemaManagementSnapshot(createdTables, createdViews);
        // end::create-schema[]
    }

    @Test
    void dropWorksheetAndFilterView() throws Exception {
        SchemaManagementSnapshot snapshot = dropWorksheetAndFilterView(sampleDriver());

        assertEquals(List.of("Projects"), snapshot.createdTables());
        assertEquals(List.of("Active Projects"), snapshot.createdViews());
    }

    private static SchemaManagementSnapshot dropWorksheetAndFilterView(OrriDriver driver) throws Exception {
        // tag::drop-schema[]
        Properties properties = new Properties();
        properties.setProperty("accessToken", "your-oauth-access-token");

        List<String> droppedRelations = new ArrayList<>();
        try (Connection connection = driver.connect(SPREADSHEET_URL, properties);
                Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table "Projects" (
                        "Name" varchar,
                        "Active" boolean,
                        "Budget" decimal(10, 2)
                    )
                    """);
            statement.execute("""
                    create view "Active Projects" as
                    select "Name", "Active"
                    from "Employees"
                    where "Active" = true
                    order by "Name"
                    """);

            statement.execute("drop view \"Active Projects\"");
            droppedRelations.add("Active Projects");

            statement.execute("drop table \"Projects\"");
            droppedRelations.add("Projects");
        }
        return new SchemaManagementSnapshot(
                droppedRelations.stream().filter("Projects"::equals).toList(),
                droppedRelations.stream().filter("Active Projects"::equals).toList());
        // end::drop-schema[]
    }

    private static int rowCount(Connection connection, String relationName) throws Exception {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("select count(*) from \"" + relationName + "\"")) {
            assertTrue(resultSet.next());
            return resultSet.getInt(1);
        }
    }

    private static OrriDriver sampleDriver() {
        return new OrriDriver(unusedUrl -> workbook(), new SpreadsheetSynchronizer() {
            @Override
            public void syncWorksheet(OrriJdbcUrl url, WorksheetSnapshot worksheet, Connection connection) {}

            @Override
            public WorksheetSnapshot createWorksheet(
                    OrriJdbcUrl url, WorksheetSnapshot worksheet, Connection connection) {
                return worksheet;
            }

            @Override
            public FilterViewDefinition createFilterView(OrriJdbcUrl url, FilterViewDefinition filterView) {
                return filterView;
            }

            @Override
            public void deleteWorksheet(OrriJdbcUrl url, WorksheetSnapshot worksheet) {}

            @Override
            public void deleteFilterView(OrriJdbcUrl url, FilterViewDefinition filterView) {}
        });
    }

    private static String normalizeTableType(String tableType) {
        return "BASE TABLE".equalsIgnoreCase(tableType) ? "TABLE" : tableType;
    }

    private static String normalizeTypeName(String typeName) {
        return "CHARACTER VARYING".equalsIgnoreCase(typeName) ? "VARCHAR" : typeName;
    }

    private static SpreadsheetSnapshot workbook() {
        WorksheetSnapshot employees = new WorksheetSnapshot(
                101,
                "Employees",
                List.of("Name", "Active", "Points"),
                List.of(ColumnType.VARCHAR, ColumnType.BOOLEAN, ColumnType.DECIMAL),
                List.of(
                        new WorksheetRow(
                                1,
                                List.of(
                                        new CellValue("Alice", "Alice", ValueKind.STRING),
                                        new CellValue(true, "TRUE", ValueKind.BOOLEAN),
                                        new CellValue(new BigDecimal("10.5"), "10.5", ValueKind.NUMBER))),
                        new WorksheetRow(
                                2,
                                List.of(
                                        new CellValue("Bob", "Bob", ValueKind.STRING),
                                        new CellValue("false", "false", ValueKind.STRING),
                                        new CellValue(new BigDecimal("7.25"), "7.25", ValueKind.NUMBER))),
                        new WorksheetRow(
                                3,
                                List.of(
                                        new CellValue("Carol", "Carol", ValueKind.STRING),
                                        new CellValue(true, "TRUE", ValueKind.BOOLEAN),
                                        CellValue.blank()))));

        FilterViewDefinition activeEmployees = new FilterViewDefinition(
                201,
                "Active Employees",
                101,
                0,
                null,
                0,
                3,
                List.of(new FilterCriterion(1, List.of(), "BOOLEAN", List.of("TRUE"))),
                List.of());

        return new SpreadsheetSnapshot(List.of(employees), List.of(activeEmployees));
    }

    private record SchemaSnapshot(List<String> relations, List<String> columns) {}

    private record SchemaManagementSnapshot(List<String> createdTables, List<String> createdViews) {}
}
