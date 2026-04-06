package io.github.rahulsom.orri;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

class OrriDriverIntegrationTest {
    private static final GsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
    private static final String DEFAULT_SPREADSHEET_ID = "1yRm-oPjuUGvfy4uhGc-woytgXQ7nSpxalWVX9djruSQ";

    @Test
    @EnabledIfSystemProperty(named = "orri.integration", matches = "true")
    void performsEndToEndLifecycleAgainstOrri() throws Exception {
        Path clientSecretPath = resourcePath("local/google-secret.json");
        String accessToken = authorize(clientSecretPath);
        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        String tableName = "Integration_" + suffix;
        String viewName = "Integration View " + suffix;

        Properties properties = new Properties();
        properties.setProperty("accessToken", accessToken);
        properties.setProperty("applicationName", "orri-integration-test");

        try (Connection connection = new OrriDriver().connect("jdbc:orri:" + spreadsheetId(), properties);
                Statement statement = connection.createStatement()) {
            assertNotNull(connection);
            assertFalse(connection.isReadOnly());

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

                List<String> initialRows = selectRows(
                        statement,
                        "select \"Name\", \"Active\", \"Score\" from \"%s\" order by \"Name\"".formatted(tableName));
                List<String> initialViewRows = selectRows(
                        statement, "select \"Name\", \"Active\" from \"%s\" order by \"Name\"".formatted(viewName));

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

                // tag::select[]
                List<String> finalRows = selectRows(
                        statement,
                        "select \"Name\", \"Active\", \"Score\" from \"%s\" order by \"Name\"".formatted(tableName));
                // end::select[]

                // tag::drop[]
                statement.execute("drop view \"%s\"".formatted(viewName));
                statement.execute("drop table \"%s\"".formatted(tableName));
                // end::drop[]

                assertEquals(
                        List.of("Ada,true,10.50", "Bob,false,7.25", "Cy,true,9.75", "Dee,false,5.50", "Eve,true,8.00"),
                        initialRows);
                assertEquals(List.of("Ada,true", "Cy,true", "Eve,true"), initialViewRows);
                assertEquals(List.of("Ada,true", "Cy,true", "Dee,true", "Eve,true"), updatedViewRows);
                assertEquals(List.of("Ada,true,10.50", "Cy,true,9.75", "Dee,true,6.50", "Eve,true,8.00"), finalRows);
            } finally {
                dropIfExists(statement, viewName, true);
                dropIfExists(statement, tableName, false);
            }
        }
    }

    private static String authorize(Path clientSecretPath) throws Exception {
        Path tokenDirectory = Path.of("orri-jdbc", "build", "google-oauth-tokens", "spreadsheets-rw");
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

    private static void dropIfExists(Statement statement, String relationName, boolean view) {
        try {
            statement.execute((view ? "drop view \"" : "drop table \"") + relationName + "\"");
        } catch (Exception ignored) {
        }
    }

    private static Path resourcePath(String resourceName) throws Exception {
        var resource = OrriDriverIntegrationTest.class.getClassLoader().getResource(resourceName);
        assertNotNull(resource);
        return Path.of(resource.toURI());
    }

    private static String spreadsheetId() {
        return System.getProperty("orri.integration.spreadsheetId", DEFAULT_SPREADSHEET_ID);
    }
}
