package io.github.rahulsom.orri;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.sheets.v4.SheetsScopes;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@EnabledIfSystemProperty(named = "orri.integration", matches = "true")
class OrriDriverIntegrationTest {
    private static final GsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
    private static final String DEFAULT_SPREADSHEET_ID = "1yRm-oPjuUGvfy4uhGc-woytgXQ7nSpxalWVX9djruSQ";

    @Test
    void performsEndToEndLifecycleAgainstOrri() throws Exception {
        Path clientSecretPath = resourcePath("local/google-secret.json");
        String accessToken = authorize(clientSecretPath);
        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        String tableName = "Integration_" + suffix;
        String viewName = "Integration View " + suffix;
        String alterTableName = "Integration Alter_" + suffix;
        String renamedAlterTableName = "Integration Alter Renamed_" + suffix;
        String renamedViewName = "Integration Renamed View " + suffix;

        Properties properties = new Properties();
        properties.setProperty(Constants.ACCESS_TOKEN_PROPERTY, accessToken);
        properties.setProperty(Constants.APPLICATION_NAME_PROPERTY, "orri-integration-test");

        try (Connection connection = new OrriDriver().connect("jdbc:orri:" + spreadsheetId(), properties);
                Statement statement = connection.createStatement()) {
            assertThat(connection).isNotNull();
            assertThat(connection.isReadOnly()).isFalse();

            try {
                // tag::create-table[]
                statement.execute("""
                        create table "%s" (
                            "Name" varchar,
                            "Active" boolean,
                            "Score" decimal(10, 2)
                        )
                        """.formatted(tableName));
                // end::create-table[]

                // tag::create-view[]
                statement.execute("""
                        create view "%s" as
                        select "Name", "Active"
                        from "%s"
                        where "Active" = true
                        order by "Name"
                        """.formatted(viewName, tableName));
                // end::create-view[]

                // tag::insert[]
                statement.executeUpdate("""
                        insert into "%s" ("Name", "Active", "Score") values
                        ('Ada', true, 10.5),
                        ('Bob', false, 7.25),
                        ('Cy', true, 9.75),
                        ('Dee', false, 5.50),
                        ('Eve', true, 8.00)
                        """.formatted(tableName));
                // end::insert[]

                List<String> initialRows = selectRows(statement, """
                                select "Name", "Active", "Score"
                                from "%s"
                                order by "Name"
                                """.formatted(tableName));
                List<String> initialViewRows = selectRows(statement, """
                                select "Name", "Active"
                                from "%s"
                                order by "Name"
                                """.formatted(viewName));

                // tag::update[]
                statement.executeUpdate("""
                        update "%s"
                        set "Active" = true, "Score" = 6.50
                        where "Name" = 'Dee'
                        """.formatted(tableName));
                // end::update[]

                List<String> updatedViewRows = selectRows(
                        statement, "select \"Name\", \"Active\" from \"%s\" order by \"Name\"".formatted(viewName));

                // tag::delete[]
                statement.executeUpdate("""
                        delete from "%s"
                        where "Name" = 'Bob'
                        """.formatted(tableName));
                // end::delete[]

                // tag::alter-table[]
                statement.execute("""
                        create table "%s" (
                            "Name" varchar,
                            "Score" decimal(10, 2)
                        )
                        """.formatted(alterTableName));
                statement.executeUpdate("""
                        insert into "%s" ("Name", "Score") values
                        ('Ada', 10.5)
                        """.formatted(alterTableName));
                statement.execute("""
                        alter table "%s"
                        rename to "%s"
                        """.formatted(alterTableName, renamedAlterTableName));
                statement.execute("""
                        alter table "%s"
                        add column "Notes" varchar
                        """.formatted(renamedAlterTableName));
                statement.executeUpdate("""
                        update "%s"
                        set "Notes" = 'top performer'
                        where "Name" = 'Ada'
                        """.formatted(renamedAlterTableName));
                statement.execute("""
                        alter table "%s"
                        alter column "Notes"
                        rename to "Comment"
                        """.formatted(renamedAlterTableName));
                statement.execute("""
                        alter table "%s"
                        drop column "Score"
                        """.formatted(renamedAlterTableName));
                // end::alter-table[]

                // tag::alter-view[]
                statement.execute("""
                        alter view "%s"
                        rename to "%s"
                        """.formatted(viewName, renamedViewName));
                statement.execute("""
                        alter view "%s" as
                        select "Name"
                        from "%s"
                        where "Name" = 'Ada'
                        """.formatted(renamedViewName, tableName));
                // end::alter-view[]

                // tag::select[]
                List<String> finalRows = selectRows(statement, """
                                select "Name", "Active", "Score"
                                from "%s"
                                order by "Name"
                                """.formatted(tableName));
                List<String> alteredTableRows = selectRows(statement, """
                                select "Name", "Comment"
                                from "%s"
                                order by "Name"
                                """.formatted(renamedAlterTableName));
                List<String> finalViewRows = selectRows(statement, """
                                select "Name"
                                from "%s"
                                order by "Name"
                                """.formatted(renamedViewName));
                // end::select[]

                // tag::drop[]
                statement.execute("drop view \"%s\"".formatted(renamedViewName));
                statement.execute("drop table \"%s\"".formatted(renamedAlterTableName));
                statement.execute("drop table \"%s\"".formatted(tableName));
                // end::drop[]

                assertThat(initialRows)
                        .isEqualTo(List.of(
                                "Ada,true,10.50", "Bob,false,7.25", "Cy,true,9.75", "Dee,false,5.50", "Eve,true,8.00"));
                assertThat(initialViewRows).isEqualTo(List.of("Ada,true", "Cy,true", "Eve,true"));
                assertThat(updatedViewRows).isEqualTo(List.of("Ada,true", "Cy,true", "Dee,true", "Eve,true"));
                assertThat(finalRows)
                        .isEqualTo(List.of("Ada,true,10.50", "Cy,true,9.75", "Dee,true,6.50", "Eve,true,8.00"));
                assertThat(alteredTableRows).isEqualTo(List.of("Ada,top performer"));
                assertThat(finalViewRows).isEqualTo(List.of("Ada"));
            } finally {
                dropIfExists(statement, renamedViewName, true);
                dropIfExists(statement, viewName, true);
                dropIfExists(statement, renamedAlterTableName, false);
                dropIfExists(statement, alterTableName, false);
                dropIfExists(statement, tableName, false);
            }
        }
    }

    @Test
    void createsAndQueriesTemporalWorksheetAgainstOrri() throws Exception {
        Path clientSecretPath = resourcePath("local/google-secret.json");
        String accessToken = authorize(clientSecretPath);
        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        String tableName = "Integration Temporal_" + suffix;

        Properties properties = new Properties();
        properties.setProperty(Constants.ACCESS_TOKEN_PROPERTY, accessToken);
        properties.setProperty(Constants.APPLICATION_NAME_PROPERTY, "orri-integration-test");

        try {
            try (Connection connection = new OrriDriver().connect("jdbc:orri:" + spreadsheetId(), properties);
                    Statement statement = connection.createStatement()) {
                statement.execute("""
                        create table "%s" (
                            "Name" varchar,
                            "EventDate" date,
                            "EventTime" time,
                            "EventTimestamp" timestamp
                        )
                        """.formatted(tableName));
                statement.executeUpdate("""
                        insert into "%s" ("Name", "EventDate", "EventTime", "EventTimestamp") values
                        ('Launch', DATE '2026-04-07', TIME '06:00:00', TIMESTAMP '2026-04-07 06:00:00'),
                        ('Review', DATE '2026-04-08', TIME '18:00:00', TIMESTAMP '2026-04-08 18:00:00')
                        """.formatted(tableName));
            }

            try (Connection connection = new OrriDriver().connect("jdbc:orri:" + spreadsheetId(), properties);
                    Statement statement = connection.createStatement()) {
                assertThat(columnTypes(statement, tableName))
                        .isEqualTo(Map.of(
                                "Name", "VARCHAR",
                                "EventDate", "DATE",
                                "EventTime", "TIME",
                                "EventTimestamp", "TIMESTAMP"));

                try (ResultSet resultSet = statement.executeQuery("""
                        select "Name", "EventDate", "EventTime", "EventTimestamp"
                        from "%s"
                        order by "Name"
                        """.formatted(tableName))) {
                    assertThat(resultSet.next()).isTrue();
                    assertThat(resultSet.getString("Name")).isEqualTo("Launch");
                    assertThat(resultSet.getObject("EventDate", LocalDate.class))
                            .isEqualTo(LocalDate.of(2026, 4, 7));
                    assertThat(resultSet.getObject("EventTime", LocalTime.class))
                            .isEqualTo(LocalTime.of(6, 0));
                    assertThat(resultSet.getObject("EventTimestamp", LocalDateTime.class))
                            .isEqualTo(LocalDateTime.of(2026, 4, 7, 6, 0));

                    assertThat(resultSet.next()).isTrue();
                    assertThat(resultSet.getString("Name")).isEqualTo("Review");
                    assertThat(resultSet.getObject("EventDate", LocalDate.class))
                            .isEqualTo(LocalDate.of(2026, 4, 8));
                    assertThat(resultSet.getObject("EventTime", LocalTime.class))
                            .isEqualTo(LocalTime.of(18, 0));
                    assertThat(resultSet.getObject("EventTimestamp", LocalDateTime.class))
                            .isEqualTo(LocalDateTime.of(2026, 4, 8, 18, 0));
                    assertThat(resultSet.next()).isFalse();
                }
            }
        } finally {
            try (Connection connection = new OrriDriver().connect("jdbc:orri:" + spreadsheetId(), properties);
                    Statement statement = connection.createStatement()) {
                dropIfExists(statement, tableName, false);
            }
        }
    }

    private static String authorize(Path clientSecretPath) throws Exception {
        Path tokenDirectory = Path.of("local", "google-oauth-tokens", "spreadsheets-rw");
        Files.createDirectories(tokenDirectory);

        try (Reader reader = Files.newBufferedReader(clientSecretPath)) {
            GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, reader);
            GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
                            new NetHttpTransport(), JSON_FACTORY, clientSecrets, List.of(SheetsScopes.SPREADSHEETS))
                    .setDataStoreFactory(new FileDataStoreFactory(tokenDirectory.toFile()))
                    .setAccessType("offline")
                    .build();

            var credential = new AuthorizationCodeInstalledApp(
                            flow,
                            new LocalServerReceiver.Builder().setPort(8888).build())
                    .authorize("orri-integration-rw");
            if (credential.getRefreshToken() != null) {
                boolean refreshed = credential.refreshToken();
                if (!refreshed && credential.getAccessToken() == null) {
                    throw new IllegalStateException("OAuth refresh did not produce an access token");
                }
            }
            if (credential.getAccessToken() == null) {
                throw new IllegalStateException("OAuth authorization did not yield an access token");
            }
            return credential.getAccessToken();
        }
    }

    private static List<String> selectRows(Statement statement, String sql) throws Exception {
        List<String> rows = new ArrayList<>();
        try (ResultSet resultSet = statement.executeQuery(sql)) {
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                List<String> values = new ArrayList<>(columnCount);
                for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
                    Object value = resultSet.getObject(columnIndex);
                    values.add(value == null ? "null" : value.toString());
                }
                rows.add(String.join(",", values));
            }
        }
        return rows;
    }

    private static Map<String, String> columnTypes(Statement statement, String relationName) throws Exception {
        Map<String, String> columnTypes = new LinkedHashMap<>();
        try (ResultSet resultSet =
                statement.executeQuery("select * from " + quoteIdentifier(relationName) + " fetch first 1 row only")) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            for (int columnIndex = 1; columnIndex <= metaData.getColumnCount(); columnIndex++) {
                columnTypes.put(
                        metaData.getColumnName(columnIndex),
                        normalizeTypeName(metaData.getColumnTypeName(columnIndex)));
            }
        }
        return columnTypes;
    }

    private static void dropIfExists(Statement statement, String relationName, boolean view) {
        try {
            statement.execute((view ? "drop view \"" : "drop table \"") + relationName + "\"");
        } catch (Exception ignored) {
        }
    }

    private static Path resourcePath(String resourceName) throws Exception {
        var resource = OrriDriverIntegrationTest.class.getClassLoader().getResource(resourceName);
        assertThat(resource).isNotNull();
        return Path.of(resource.toURI());
    }

    private static String spreadsheetId() {
        return System.getProperty("orri.integration.spreadsheetId", DEFAULT_SPREADSHEET_ID);
    }

    private static String normalizeTypeName(String typeName) {
        return "CHARACTER VARYING".equalsIgnoreCase(typeName) ? "VARCHAR" : typeName;
    }

    private static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}
