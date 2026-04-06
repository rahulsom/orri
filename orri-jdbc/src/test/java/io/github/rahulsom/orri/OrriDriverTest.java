package io.github.rahulsom.orri;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class OrriDriverTest {
    @Test
    void acceptsOrriUrls() {
        OrriDriver driver = new OrriDriver(unusedUrl -> workbook(), noOpSynchronizer());

        assertTrue(driver.acceptsURL("jdbc:orri:spreadsheet-id"));
        assertFalse(driver.acceptsURL("jdbc:h2:mem:test"));
    }

    @Test
    void exposesConnectionProperties() {
        OrriDriver driver = new OrriDriver(unusedUrl -> workbook(), noOpSynchronizer());

        DriverPropertyInfo[] propertyInfos = driver.getPropertyInfo("jdbc:orri:test", new Properties());

        assertEquals(6, propertyInfos.length);
        assertEquals("apiKey", propertyInfos[0].name);
        assertEquals("readOnly", propertyInfos[4].name);
        assertEquals("applicationName", propertyInfos[5].name);
    }

    @Test
    void materializesSheetsAndFilterViews() throws Exception {
        OrriDriver driver = new OrriDriver(unusedUrl -> workbook(), noOpSynchronizer());

        try (Connection connection = driver.connect("jdbc:orri:test-sheet", new Properties())) {
            assertNotNull(connection);
            assertFalse(connection.isReadOnly());

            try (ResultSet resultSet = connection
                    .createStatement()
                    .executeQuery("select \"Name\", \"Active\", \"Points\" from \"Employees\" order by \"Name\"")) {
                assertTrue(resultSet.next());
                assertEquals("Alice", resultSet.getString(1));
                assertTrue(resultSet.getBoolean(2));
                assertEquals(new BigDecimal("10.5000000000"), resultSet.getBigDecimal(3));

                assertTrue(resultSet.next());
                assertEquals("Bob", resultSet.getString(1));
                assertFalse(resultSet.getBoolean(2));
                assertEquals(new BigDecimal("7.2500000000"), resultSet.getBigDecimal(3));

                assertTrue(resultSet.next());
                assertEquals("Carol", resultSet.getString(1));
                assertTrue(resultSet.getBoolean(2));
                assertNull(resultSet.getBigDecimal(3));
                assertFalse(resultSet.next());
            }

            try (ResultSet resultSet = connection
                    .createStatement()
                    .executeQuery("select \"Name\" from \"Active Employees\" order by \"Name\"")) {
                assertTrue(resultSet.next());
                assertEquals("Alice", resultSet.getString(1));
                assertTrue(resultSet.next());
                assertEquals("Carol", resultSet.getString(1));
                assertFalse(resultSet.next());
            }
        }
    }

    @Test
    void reportsFilterViewsAsViews() throws Exception {
        OrriDriver driver = new OrriDriver(unusedUrl -> workbook(), noOpSynchronizer());

        try (Connection connection = driver.connect("jdbc:orri:test-sheet", new Properties())) {
            DatabaseMetaData metaData = connection.getMetaData();
            assertEquals("jdbc:orri:test-sheet", metaData.getURL());

            try (ResultSet tables = metaData.getTables(null, null, null, null)) {
                boolean foundView = false;
                while (tables.next()) {
                    if ("Active Employees".equals(tables.getString("TABLE_NAME"))) {
                        foundView = true;
                        assertEquals("VIEW", tables.getString("TABLE_TYPE"));
                    }
                }
                assertTrue(foundView);
            }
        }
    }

    @Test
    void parsesConnectionPropertiesFromUrlAndProperties() throws Exception {
        OrriJdbcUrl jdbcUrl = OrriJdbcUrl.parse(
                "jdbc:orri:spreadsheet-id;apiKey=url-key",
                properties("applicationName", "test-app", "apiKey", "property-key"));

        assertEquals("spreadsheet-id", jdbcUrl.spreadsheetId());
        assertEquals("property-key", jdbcUrl.property("apiKey"));
        assertEquals("test-app", jdbcUrl.applicationName());
    }

    @Test
    void supportsInsertUpdateAndDeleteByDefault() throws Exception {
        RecordingSynchronizer synchronizer = new RecordingSynchronizer();
        OrriDriver driver = new OrriDriver(unusedUrl -> workbook(), synchronizer);

        try (Connection connection = driver.connect("jdbc:orri:test-sheet", properties("accessToken", "token"))) {
            assertFalse(connection.isReadOnly());

            assertEquals(
                    1,
                    connection
                            .createStatement()
                            .executeUpdate(
                                    "insert into \"Employees\" (\"Name\", \"Active\", \"Points\") values ('Dave', true, 2.5)"));
            assertEquals(
                    1,
                    connection
                            .createStatement()
                            .executeUpdate("update \"Employees\" set \"Active\" = true where \"Name\" = 'Bob'"));
            assertEquals(
                    1,
                    connection.createStatement().executeUpdate("delete from \"Employees\" where \"Name\" = 'Carol'"));

            try (ResultSet resultSet =
                    connection.createStatement().executeQuery("select \"Name\" from \"Employees\" order by \"Name\"")) {
                assertTrue(resultSet.next());
                assertEquals("Alice", resultSet.getString(1));
                assertTrue(resultSet.next());
                assertEquals("Bob", resultSet.getString(1));
                assertTrue(resultSet.next());
                assertEquals("Dave", resultSet.getString(1));
                assertFalse(resultSet.next());
            }

            try (ResultSet resultSet = connection
                    .createStatement()
                    .executeQuery("select \"Name\" from \"Active Employees\" order by \"Name\"")) {
                assertTrue(resultSet.next());
                assertEquals("Alice", resultSet.getString(1));
                assertTrue(resultSet.next());
                assertEquals("Bob", resultSet.getString(1));
                assertTrue(resultSet.next());
                assertEquals("Dave", resultSet.getString(1));
                assertFalse(resultSet.next());
            }
        }

        assertEquals(List.of("Employees", "Employees", "Employees"), synchronizer.syncedWorksheets);
    }

    @Test
    void rejectsWritesWhenConnectionIsReadOnly() throws Exception {
        OrriDriver driver = new OrriDriver(unusedUrl -> workbook(), noOpSynchronizer());

        try (Connection connection =
                driver.connect("jdbc:orri:test-sheet;readOnly=true", properties("accessToken", "token"))) {
            assertTrue(connection.isReadOnly());
            SQLException exception = org.junit.jupiter.api.Assertions.assertThrows(SQLException.class, () -> connection
                    .createStatement()
                    .executeUpdate("delete from \"Employees\" where \"Name\" = 'Alice'"));
            assertTrue(exception.getMessage().contains("read-only"));
        }
    }

    @Test
    void createTableCreatesWorksheetInSynchronizer() throws Exception {
        RecordingSynchronizer synchronizer = new RecordingSynchronizer();
        OrriDriver driver = new OrriDriver(unusedUrl -> workbook(), synchronizer);

        try (Connection connection = driver.connect("jdbc:orri:test-sheet", properties("accessToken", "token"))) {
            connection.createStatement().execute("""
                    create table "Projects" (
                        "Name" varchar,
                        "Active" boolean,
                        "Budget" decimal(10, 2)
                    )
                    """);

            assertEquals(List.of("Projects"), synchronizer.createdWorksheets);
            assertEquals(0, rowCount(connection, "Projects"));
        }
    }

    @Test
    void createViewCreatesFilterViewInSynchronizer() throws Exception {
        RecordingSynchronizer synchronizer = new RecordingSynchronizer();
        OrriDriver driver = new OrriDriver(unusedUrl -> workbook(), synchronizer);

        try (Connection connection = driver.connect("jdbc:orri:test-sheet", properties("accessToken", "token"))) {
            connection.createStatement().execute("""
                    create view "Active By Name" as
                    select "Name", "Active"
                    from "Employees"
                    where "Active" = true
                    order by "Name"
                    """);

            assertEquals(List.of("Active By Name"), synchronizer.createdFilterViews);
            assertEquals(
                    List.of(new FilterCriterion(1, List.of("FALSE", "false"), null, List.of())),
                    synchronizer.createdFilterViewDefinitions.getFirst().criteria());
            try (ResultSet resultSet = connection
                    .createStatement()
                    .executeQuery("select \"Name\" from \"Active By Name\" order by \"Name\"")) {
                assertTrue(resultSet.next());
                assertEquals("Alice", resultSet.getString(1));
                assertTrue(resultSet.next());
                assertEquals("Carol", resultSet.getString(1));
                assertFalse(resultSet.next());
            }
        }
    }

    @Test
    void dropTableDeletesWorksheetAndDependentViews() throws Exception {
        RecordingSynchronizer synchronizer = new RecordingSynchronizer();
        OrriDriver driver = new OrriDriver(unusedUrl -> workbook(), synchronizer);

        try (Connection connection = driver.connect("jdbc:orri:test-sheet", properties("accessToken", "token"))) {
            assertFalse(connection.createStatement().execute("drop table \"Employees\""));

            SQLException exception = org.junit.jupiter.api.Assertions.assertThrows(
                    SQLException.class, () -> connection.createStatement().executeQuery("select * from \"Employees\""));
            assertNotNull(exception);
        }

        assertEquals(List.of("Active Employees"), synchronizer.deletedFilterViews);
        assertEquals(List.of("Employees"), synchronizer.deletedWorksheets);
    }

    @Test
    void dropViewDeletesRemoteFilterView() throws Exception {
        RecordingSynchronizer synchronizer = new RecordingSynchronizer();
        OrriDriver driver = new OrriDriver(unusedUrl -> workbook(), synchronizer);

        try (Connection connection = driver.connect("jdbc:orri:test-sheet", properties("accessToken", "token"))) {
            assertFalse(connection.createStatement().execute("drop view \"Active Employees\""));

            SQLException exception = org.junit.jupiter.api.Assertions.assertThrows(
                    SQLException.class,
                    () -> connection.createStatement().executeQuery("select * from \"Active Employees\""));
            assertNotNull(exception);
        }

        assertEquals(List.of("Active Employees"), synchronizer.deletedFilterViews);
    }

    @Test
    void alterTableRenameRenamesWorksheetInSynchronizer() throws Exception {
        RecordingSynchronizer synchronizer = new RecordingSynchronizer();
        OrriDriver driver = new OrriDriver(unusedUrl -> workbook(), synchronizer);

        try (Connection connection = driver.connect("jdbc:orri:test-sheet", properties("accessToken", "token"))) {
            assertFalse(connection.createStatement().execute("alter table \"Employees\" rename to \"People\""));

            try (ResultSet resultSet =
                    connection.createStatement().executeQuery("select \"Name\" from \"People\" order by \"Name\"")) {
                assertTrue(resultSet.next());
                assertEquals("Alice", resultSet.getString(1));
                assertTrue(resultSet.next());
                assertEquals("Bob", resultSet.getString(1));
                assertFalse(connection.createStatement().execute("drop view \"Active Employees\""));
            }
        }

        assertEquals(List.of("Employees->People"), synchronizer.renamedWorksheets);
    }

    @Test
    void alterTableRenameColumnSynchronizesWorksheet() throws Exception {
        RecordingSynchronizer synchronizer = new RecordingSynchronizer();
        OrriDriver driver = new OrriDriver(unusedUrl -> workbookWithoutViews(), synchronizer);

        try (Connection connection = driver.connect("jdbc:orri:test-sheet", properties("accessToken", "token"))) {
            assertFalse(connection
                    .createStatement()
                    .execute("alter table \"Employees\" alter column \"Points\" rename to \"Score\""));

            try (ResultSet resultSet = connection
                    .createStatement()
                    .executeQuery("select \"Name\", \"Score\" from \"Employees\" order by \"Name\"")) {
                assertTrue(resultSet.next());
                assertEquals("Alice", resultSet.getString(1));
                assertEquals(new BigDecimal("10.5000000000"), resultSet.getBigDecimal(2));
            }
        }

        assertEquals(List.of("Employees"), synchronizer.syncedWorksheets);
    }

    @Test
    void alterViewRenameRenamesFilterViewInSynchronizer() throws Exception {
        RecordingSynchronizer synchronizer = new RecordingSynchronizer();
        OrriDriver driver = new OrriDriver(unusedUrl -> workbook(), synchronizer);

        try (Connection connection = driver.connect("jdbc:orri:test-sheet", properties("accessToken", "token"))) {
            assertFalse(connection
                    .createStatement()
                    .execute("alter view \"Active Employees\" rename to \"Enabled Employees\""));

            try (ResultSet resultSet = connection
                    .createStatement()
                    .executeQuery("select \"Name\" from \"Enabled Employees\" order by \"Name\"")) {
                assertTrue(resultSet.next());
                assertEquals("Alice", resultSet.getString(1));
                assertTrue(resultSet.next());
                assertEquals("Carol", resultSet.getString(1));
                assertFalse(resultSet.next());
            }
        }

        assertEquals(List.of("Active Employees->Enabled Employees"), synchronizer.updatedFilterViews);
    }

    @Test
    void alterViewAsReplacesFilterViewDefinitionInSynchronizer() throws Exception {
        RecordingSynchronizer synchronizer = new RecordingSynchronizer();
        OrriDriver driver = new OrriDriver(unusedUrl -> workbook(), synchronizer);

        try (Connection connection = driver.connect("jdbc:orri:test-sheet", properties("accessToken", "token"))) {
            assertFalse(connection.createStatement().execute("""
                    alter view "Active Employees" as
                    select "Name"
                    from "Employees"
                    where "Name" = 'Bob'
                    """));

            try (ResultSet resultSet = connection
                    .createStatement()
                    .executeQuery("select \"Name\" from \"Active Employees\" order by \"Name\"")) {
                assertTrue(resultSet.next());
                assertEquals("Bob", resultSet.getString(1));
                assertFalse(resultSet.next());
            }
        }

        assertEquals(List.of("Active Employees->Active Employees"), synchronizer.updatedFilterViews);
        assertEquals(
                List.of(new FilterCriterion(0, List.of(), "TEXT_EQ", List.of("Bob"))),
                synchronizer.updatedFilterViewDefinitions.getFirst().criteria());
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

    private static SpreadsheetSnapshot workbookWithoutViews() {
        WorksheetSnapshot employees = workbook().worksheets().getFirst();
        return new SpreadsheetSnapshot(List.of(employees), List.of());
    }

    private static Properties properties(String... values) {
        Properties properties = new Properties();
        for (int index = 0; index < values.length; index += 2) {
            properties.setProperty(values[index], values[index + 1]);
        }
        return properties;
    }

    private static int rowCount(Connection connection, String relationName) throws Exception {
        try (ResultSet resultSet =
                connection.createStatement().executeQuery("select count(*) from \"" + relationName + "\"")) {
            assertTrue(resultSet.next());
            return resultSet.getInt(1);
        }
    }

    private static SpreadsheetSynchronizer noOpSynchronizer() {
        return new SpreadsheetSynchronizer() {
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

            @Override
            public WorksheetSnapshot renameWorksheet(OrriJdbcUrl url, WorksheetSnapshot worksheet, String newName) {
                return new WorksheetSnapshot(
                        worksheet.sheetId(),
                        newName,
                        worksheet.columnNames(),
                        worksheet.columnTypes(),
                        worksheet.rows());
            }

            @Override
            public FilterViewDefinition updateFilterView(
                    OrriJdbcUrl url, FilterViewDefinition existingFilterView, FilterViewDefinition updatedFilterView) {
                return updatedFilterView;
            }
        };
    }

    private static final class RecordingSynchronizer implements SpreadsheetSynchronizer {
        private final List<String> syncedWorksheets = new ArrayList<>();
        private final List<String> createdWorksheets = new ArrayList<>();
        private final List<String> createdFilterViews = new ArrayList<>();
        private final List<FilterViewDefinition> createdFilterViewDefinitions = new ArrayList<>();
        private final List<String> deletedWorksheets = new ArrayList<>();
        private final List<String> deletedFilterViews = new ArrayList<>();
        private final List<String> renamedWorksheets = new ArrayList<>();
        private final List<String> updatedFilterViews = new ArrayList<>();
        private final List<FilterViewDefinition> updatedFilterViewDefinitions = new ArrayList<>();

        @Override
        public void syncWorksheet(OrriJdbcUrl url, WorksheetSnapshot worksheet, Connection connection) {
            syncedWorksheets.add(worksheet.name());
        }

        @Override
        public WorksheetSnapshot createWorksheet(OrriJdbcUrl url, WorksheetSnapshot worksheet, Connection connection) {
            createdWorksheets.add(worksheet.name());
            return new WorksheetSnapshot(
                    999 + createdWorksheets.size(),
                    worksheet.name(),
                    worksheet.columnNames(),
                    worksheet.columnTypes(),
                    worksheet.rows());
        }

        @Override
        public FilterViewDefinition createFilterView(OrriJdbcUrl url, FilterViewDefinition filterView) {
            createdFilterViews.add(filterView.name());
            createdFilterViewDefinitions.add(filterView);
            return new FilterViewDefinition(
                    300 + createdFilterViews.size(),
                    filterView.name(),
                    filterView.sourceSheetId(),
                    filterView.startRowIndex(),
                    filterView.endRowIndex(),
                    filterView.startColumnIndex(),
                    filterView.endColumnIndex(),
                    filterView.criteria(),
                    filterView.sortKeys());
        }

        @Override
        public void deleteWorksheet(OrriJdbcUrl url, WorksheetSnapshot worksheet) {
            deletedWorksheets.add(worksheet.name());
        }

        @Override
        public void deleteFilterView(OrriJdbcUrl url, FilterViewDefinition filterView) {
            deletedFilterViews.add(filterView.name());
        }

        @Override
        public WorksheetSnapshot renameWorksheet(OrriJdbcUrl url, WorksheetSnapshot worksheet, String newName) {
            renamedWorksheets.add(worksheet.name() + "->" + newName);
            return new WorksheetSnapshot(
                    worksheet.sheetId(), newName, worksheet.columnNames(), worksheet.columnTypes(), worksheet.rows());
        }

        @Override
        public FilterViewDefinition updateFilterView(
                OrriJdbcUrl url, FilterViewDefinition existingFilterView, FilterViewDefinition updatedFilterView) {
            updatedFilterViews.add(existingFilterView.name() + "->" + updatedFilterView.name());
            updatedFilterViewDefinitions.add(updatedFilterView);
            return new FilterViewDefinition(
                    existingFilterView.filterViewId(),
                    updatedFilterView.name(),
                    updatedFilterView.sourceSheetId(),
                    updatedFilterView.startRowIndex(),
                    updatedFilterView.endRowIndex(),
                    updatedFilterView.startColumnIndex(),
                    updatedFilterView.endColumnIndex(),
                    updatedFilterView.criteria(),
                    updatedFilterView.sortKeys());
        }
    }
}
