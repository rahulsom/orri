package io.github.rahulsom.orri;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class OrriDriverTest {
    @Test
    void acceptsOrriUrls() {
        OrriDriver driver = new OrriDriver(unusedUrl -> workbook(), noOpSynchronizer());

        assertThat(driver.acceptsURL("jdbc:orri:spreadsheet-id")).isTrue();
        assertThat(driver.acceptsURL("jdbc:h2:mem:test")).isFalse();
    }

    @Test
    void exposesConnectionProperties() {
        OrriDriver driver = new OrriDriver(unusedUrl -> workbook(), noOpSynchronizer());

        DriverPropertyInfo[] propertyInfos = driver.getPropertyInfo("jdbc:orri:test", new Properties());

        assertThat(propertyInfos).hasSize(6);
        assertThat(propertyInfos[0].name).isEqualTo("apiKey");
        assertThat(propertyInfos[4].name).isEqualTo("readOnly");
        assertThat(propertyInfos[5].name).isEqualTo("applicationName");
    }

    @Test
    void materializesSheetsAndFilterViews() throws Exception {
        OrriDriver driver = new OrriDriver(unusedUrl -> workbook(), noOpSynchronizer());

        try (Connection connection = driver.connect("jdbc:orri:test-sheet", new Properties())) {
            assertThat(connection).isNotNull();
            assertThat(connection.isReadOnly()).isFalse();

            try (ResultSet resultSet = connection
                    .createStatement()
                    .executeQuery("select \"Name\", \"Active\", \"Points\" from \"Employees\" order by \"Name\"")) {
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getString(1)).isEqualTo("Alice");
                assertThat(resultSet.getBoolean(2)).isTrue();
                assertThat(resultSet.getBigDecimal(3)).isEqualTo(new BigDecimal("10.5000000000"));

                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getString(1)).isEqualTo("Bob");
                assertThat(resultSet.getBoolean(2)).isFalse();
                assertThat(resultSet.getBigDecimal(3)).isEqualTo(new BigDecimal("7.2500000000"));

                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getString(1)).isEqualTo("Carol");
                assertThat(resultSet.getBoolean(2)).isTrue();
                assertThat(resultSet.getBigDecimal(3)).isNull();
                assertThat(resultSet.next()).isFalse();
            }

            try (ResultSet resultSet = connection
                    .createStatement()
                    .executeQuery("select \"Name\" from \"Active Employees\" order by \"Name\"")) {
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getString(1)).isEqualTo("Alice");
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getString(1)).isEqualTo("Carol");
                assertThat(resultSet.next()).isFalse();
            }
        }
    }

    @Test
    void reportsFilterViewsAsViews() throws Exception {
        OrriDriver driver = new OrriDriver(unusedUrl -> workbook(), noOpSynchronizer());

        try (Connection connection = driver.connect("jdbc:orri:test-sheet", new Properties())) {
            DatabaseMetaData metaData = connection.getMetaData();
            assertThat(metaData.getURL()).isEqualTo("jdbc:orri:test-sheet");

            try (ResultSet tables = metaData.getTables(null, null, null, null)) {
                boolean foundView = false;
                while (tables.next()) {
                    if ("Active Employees".equals(tables.getString("TABLE_NAME"))) {
                        foundView = true;
                        assertThat(tables.getString("TABLE_TYPE")).isEqualTo("VIEW");
                    }
                }
                assertThat(foundView).isTrue();
            }
        }
    }

    @Test
    void materializesTemporalColumns() throws Exception {
        OrriDriver driver = new OrriDriver(unusedUrl -> temporalWorkbook(), noOpSynchronizer());

        try (Connection connection = driver.connect("jdbc:orri:test-sheet", new Properties())) {
            try (ResultSet resultSet = connection
                    .createStatement()
                    .executeQuery("select \"Date\", \"Time\", \"Timestamp\" from \"Schedule\"")) {
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getObject(1, LocalDate.class)).isEqualTo(LocalDate.of(2026, 4, 7));
                assertThat(resultSet.getObject(2, LocalTime.class)).isEqualTo(LocalTime.of(9, 15, 30));
                assertThat(resultSet.getObject(3, LocalDateTime.class))
                        .isEqualTo(LocalDateTime.of(2026, 4, 7, 9, 15, 30));
                assertThat(resultSet.next()).isFalse();
            }

            try (ResultSet columns = connection.getMetaData().getColumns(null, null, "Schedule", "%")) {
                assertThat(columns.next()).isTrue();
                assertThat(columns.getString("COLUMN_NAME")).isEqualTo("Date");
                assertThat(columns.getInt("DATA_TYPE")).isEqualTo(Types.DATE);
                assertThat(columns.next()).isTrue();
                assertThat(columns.getString("COLUMN_NAME")).isEqualTo("Time");
                assertThat(columns.getInt("DATA_TYPE")).isEqualTo(Types.TIME);
                assertThat(columns.next()).isTrue();
                assertThat(columns.getString("COLUMN_NAME")).isEqualTo("Timestamp");
                assertThat(columns.getInt("DATA_TYPE")).isEqualTo(Types.TIMESTAMP);
                assertThat(columns.next()).isFalse();
            }
        }
    }

    @Test
    void parsesConnectionPropertiesFromUrlAndProperties() throws Exception {
        OrriJdbcUrl jdbcUrl = OrriJdbcUrl.parse(
                "jdbc:orri:spreadsheet-id;apiKey=url-key",
                properties("applicationName", "test-app", "apiKey", "property-key"));

        assertThat(jdbcUrl.spreadsheetId()).isEqualTo("spreadsheet-id");
        assertThat(jdbcUrl.property("apiKey")).isEqualTo("property-key");
        assertThat(jdbcUrl.applicationName()).isEqualTo("test-app");
    }

    @Test
    void supportsInsertUpdateAndDeleteByDefault() throws Exception {
        RecordingSynchronizer synchronizer = new RecordingSynchronizer();
        OrriDriver driver = new OrriDriver(unusedUrl -> workbook(), synchronizer);

        try (Connection connection = driver.connect("jdbc:orri:test-sheet", properties("accessToken", "token"))) {
            assertThat(connection.isReadOnly()).isFalse();

            assertThat(
                            connection
                                    .createStatement()
                                    .executeUpdate(
                                            "insert into \"Employees\" (\"Name\", \"Active\", \"Points\") values ('Dave', true, 2.5)"))
                    .isEqualTo(1);
            assertThat(connection
                            .createStatement()
                            .executeUpdate("update \"Employees\" set \"Active\" = true where \"Name\" = 'Bob'"))
                    .isEqualTo(1);
            assertThat(connection.createStatement().executeUpdate("delete from \"Employees\" where \"Name\" = 'Carol'"))
                    .isEqualTo(1);

            try (ResultSet resultSet =
                    connection.createStatement().executeQuery("select \"Name\" from \"Employees\" order by \"Name\"")) {
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getString(1)).isEqualTo("Alice");
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getString(1)).isEqualTo("Bob");
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getString(1)).isEqualTo("Dave");
                assertThat(resultSet.next()).isFalse();
            }

            try (ResultSet resultSet = connection
                    .createStatement()
                    .executeQuery("select \"Name\" from \"Active Employees\" order by \"Name\"")) {
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getString(1)).isEqualTo("Alice");
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getString(1)).isEqualTo("Bob");
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getString(1)).isEqualTo("Dave");
                assertThat(resultSet.next()).isFalse();
            }
        }

        assertThat(synchronizer.syncedWorksheets).isEqualTo(List.of("Employees", "Employees", "Employees"));
    }

    @Test
    void rejectsWritesWhenConnectionIsReadOnly() throws Exception {
        OrriDriver driver = new OrriDriver(unusedUrl -> workbook(), noOpSynchronizer());

        try (Connection connection =
                driver.connect("jdbc:orri:test-sheet;readOnly=true", properties("accessToken", "token"))) {
            assertThat(connection.isReadOnly()).isTrue();
            assertThatThrownBy(() -> connection
                            .createStatement()
                            .executeUpdate("delete from \"Employees\" where \"Name\" = 'Alice'"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("read-only");
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

            assertThat(synchronizer.createdWorksheets).isEqualTo(List.of("Projects"));
            assertThat(rowCount(connection, "Projects")).isZero();
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

            assertThat(synchronizer.createdFilterViews).isEqualTo(List.of("Active By Name"));
            assertThat(synchronizer.createdFilterViewDefinitions.getFirst().criteria())
                    .isEqualTo(List.of(new FilterCriterion(1, List.of("FALSE", "false"), null, List.of())));
            try (ResultSet resultSet = connection
                    .createStatement()
                    .executeQuery("select \"Name\" from \"Active By Name\" order by \"Name\"")) {
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getString(1)).isEqualTo("Alice");
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getString(1)).isEqualTo("Carol");
                assertThat(resultSet.next()).isFalse();
            }
        }
    }

    @Test
    void dropTableDeletesWorksheetAndDependentViews() throws Exception {
        RecordingSynchronizer synchronizer = new RecordingSynchronizer();
        OrriDriver driver = new OrriDriver(unusedUrl -> workbook(), synchronizer);

        try (Connection connection = driver.connect("jdbc:orri:test-sheet", properties("accessToken", "token"))) {
            assertThat(connection.createStatement().execute("drop table \"Employees\""))
                    .isFalse();
            assertThatThrownBy(() -> connection.createStatement().executeQuery("select * from \"Employees\""))
                    .isInstanceOf(SQLException.class);
        }

        assertThat(synchronizer.deletedFilterViews).isEqualTo(List.of("Active Employees"));
        assertThat(synchronizer.deletedWorksheets).isEqualTo(List.of("Employees"));
    }

    @Test
    void dropViewDeletesRemoteFilterView() throws Exception {
        RecordingSynchronizer synchronizer = new RecordingSynchronizer();
        OrriDriver driver = new OrriDriver(unusedUrl -> workbook(), synchronizer);

        try (Connection connection = driver.connect("jdbc:orri:test-sheet", properties("accessToken", "token"))) {
            assertThat(connection.createStatement().execute("drop view \"Active Employees\""))
                    .isFalse();
            assertThatThrownBy(() -> connection.createStatement().executeQuery("select * from \"Active Employees\""))
                    .isInstanceOf(SQLException.class);
        }

        assertThat(synchronizer.deletedFilterViews).isEqualTo(List.of("Active Employees"));
    }

    @Test
    void alterTableRenameRenamesWorksheetInSynchronizer() throws Exception {
        RecordingSynchronizer synchronizer = new RecordingSynchronizer();
        OrriDriver driver = new OrriDriver(unusedUrl -> workbook(), synchronizer);

        try (Connection connection = driver.connect("jdbc:orri:test-sheet", properties("accessToken", "token"))) {
            assertThat(connection.createStatement().execute("alter table \"Employees\" rename to \"People\""))
                    .isFalse();

            try (ResultSet resultSet =
                    connection.createStatement().executeQuery("select \"Name\" from \"People\" order by \"Name\"")) {
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getString(1)).isEqualTo("Alice");
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getString(1)).isEqualTo("Bob");
                assertThat(connection.createStatement().execute("drop view \"Active Employees\""))
                        .isFalse();
            }
        }

        assertThat(synchronizer.renamedWorksheets).isEqualTo(List.of("Employees->People"));
    }

    @Test
    void alterTableRenameColumnSynchronizesWorksheet() throws Exception {
        RecordingSynchronizer synchronizer = new RecordingSynchronizer();
        OrriDriver driver = new OrriDriver(unusedUrl -> workbookWithoutViews(), synchronizer);

        try (Connection connection = driver.connect("jdbc:orri:test-sheet", properties("accessToken", "token"))) {
            assertThat(connection
                            .createStatement()
                            .execute("alter table \"Employees\" alter column \"Points\" rename to \"Score\""))
                    .isFalse();

            try (ResultSet resultSet = connection
                    .createStatement()
                    .executeQuery("select \"Name\", \"Score\" from \"Employees\" order by \"Name\"")) {
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getString(1)).isEqualTo("Alice");
                assertThat(resultSet.getBigDecimal(2)).isEqualTo(new BigDecimal("10.5000000000"));
            }
        }

        assertThat(synchronizer.syncedWorksheets).isEqualTo(List.of("Employees"));
    }

    @Test
    void alterViewRenameRenamesFilterViewInSynchronizer() throws Exception {
        RecordingSynchronizer synchronizer = new RecordingSynchronizer();
        OrriDriver driver = new OrriDriver(unusedUrl -> workbook(), synchronizer);

        try (Connection connection = driver.connect("jdbc:orri:test-sheet", properties("accessToken", "token"))) {
            assertThat(connection
                            .createStatement()
                            .execute("alter view \"Active Employees\" rename to \"Enabled Employees\""))
                    .isFalse();

            try (ResultSet resultSet = connection
                    .createStatement()
                    .executeQuery("select \"Name\" from \"Enabled Employees\" order by \"Name\"")) {
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getString(1)).isEqualTo("Alice");
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getString(1)).isEqualTo("Carol");
                assertThat(resultSet.next()).isFalse();
            }
        }

        assertThat(synchronizer.updatedFilterViews).isEqualTo(List.of("Active Employees->Enabled Employees"));
    }

    @Test
    void alterViewAsReplacesFilterViewDefinitionInSynchronizer() throws Exception {
        RecordingSynchronizer synchronizer = new RecordingSynchronizer();
        OrriDriver driver = new OrriDriver(unusedUrl -> workbook(), synchronizer);

        try (Connection connection = driver.connect("jdbc:orri:test-sheet", properties("accessToken", "token"))) {
            assertThat(connection.createStatement().execute("""
                            alter view "Active Employees" as
                            select "Name"
                            from "Employees"
                            where "Name" = 'Bob'
                            """)).isFalse();

            try (ResultSet resultSet = connection
                    .createStatement()
                    .executeQuery("select \"Name\" from \"Active Employees\" order by \"Name\"")) {
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.getString(1)).isEqualTo("Bob");
                assertThat(resultSet.next()).isFalse();
            }
        }

        assertThat(synchronizer.updatedFilterViews).isEqualTo(List.of("Active Employees->Active Employees"));
        assertThat(synchronizer.updatedFilterViewDefinitions.getFirst().criteria())
                .isEqualTo(List.of(new FilterCriterion(0, List.of(), "TEXT_EQ", List.of("Bob"))));
    }

    @Test
    void readsTemporalWorksheetSnapshotFromJdbc() throws Exception {
        try (Connection connection =
                DriverManager.getConnection("jdbc:h2:mem:test-temporal;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false")) {
            connection.createStatement().execute("""
                    create table "Schedule" (
                        "Date" date,
                        "Time" time,
                        "Timestamp" timestamp
                    )
                    """);
            connection.createStatement().execute("""
                    insert into "Schedule" values
                    (DATE '2026-04-07', TIME '09:15:30', TIMESTAMP '2026-04-07 09:15:30')
                    """);

            WorksheetSnapshot snapshot = OrriDatabase.readWorksheetSnapshot(connection, "Schedule", 42);

            assertThat(snapshot.columnTypes())
                    .isEqualTo(List.of(ColumnType.DATE, ColumnType.TIME, ColumnType.TIMESTAMP));
            assertThat(SheetsTemporalSupport.asLocalDate(
                            snapshot.rows().getFirst().cells().get(0).typedValue()))
                    .isEqualTo(LocalDate.of(2026, 4, 7));
            assertThat(SheetsTemporalSupport.asLocalTime(
                            snapshot.rows().getFirst().cells().get(1).typedValue()))
                    .isEqualTo(LocalTime.of(9, 15, 30));
            assertThat(SheetsTemporalSupport.asLocalDateTime(
                            snapshot.rows().getFirst().cells().get(2).typedValue()))
                    .isEqualTo(LocalDateTime.of(2026, 4, 7, 9, 15, 30));
        }
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

    private static SpreadsheetSnapshot temporalWorkbook() {
        WorksheetSnapshot schedule = new WorksheetSnapshot(
                102,
                "Schedule",
                List.of("Date", "Time", "Timestamp"),
                List.of(ColumnType.DATE, ColumnType.TIME, ColumnType.TIMESTAMP),
                List.of(new WorksheetRow(
                        1,
                        List.of(
                                new CellValue(LocalDate.of(2026, 4, 7), "2026-04-07", ValueKind.DATE),
                                new CellValue(LocalTime.of(9, 15, 30), "09:15:30", ValueKind.TIME),
                                new CellValue(
                                        LocalDateTime.of(2026, 4, 7, 9, 15, 30),
                                        "2026-04-07 09:15:30",
                                        ValueKind.TIMESTAMP)))));
        return new SpreadsheetSnapshot(List.of(schedule), List.of());
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
            assertThat(resultSet.next()).isTrue();
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
