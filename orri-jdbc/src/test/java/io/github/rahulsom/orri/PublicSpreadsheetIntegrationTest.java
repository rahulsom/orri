package io.github.rahulsom.orri;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@EnabledIfSystemProperty(named = "orri.integration", matches = "true")
class PublicSpreadsheetIntegrationTest {

    @Test
    void readsTdfLeaderboard() throws Exception {
        Properties properties = new Properties();
        String apiKey = System.getProperty("orri.integration.apiKey", System.getenv("GOOGLE_API_KEY"));
        if (apiKey != null && !apiKey.isBlank()) {
            properties.setProperty(Constants.API_KEY_PROPERTY, apiKey);
        }
        properties.setProperty(Constants.APPLICATION_NAME_PROPERTY, "orri-integration-test");

        try (Connection connection =
                        new OrriDriver().connect("jdbc:orri:1KDVG4RHt3GNbitF74NLYO3OtsGFJotKzlmWe_XUc-iM", properties);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("""
                        select "column_3", "column_4"
                        from "Sheet1"
                        where CASE WHEN "column_4" REGEXP '^[0-9]+$' THEN CAST("column_4" AS DECIMAL) ELSE 0 END >= 20
                        order by CASE WHEN "column_4" REGEXP '^[0-9]+$' THEN CAST("column_4" AS DECIMAL) ELSE 0 END desc
                        """)) {
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getString("column_3")).isEqualTo("Mark Cavendish");
            assertThat(resultSet.getBigDecimal("column_4")).isEqualByComparingTo("35");

            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getString("column_3")).isEqualTo("Eddy Merckx");
            assertThat(resultSet.getBigDecimal("column_4")).isEqualByComparingTo("34");

            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getString("column_3")).isEqualTo("Bernard Hinault");
            assertThat(resultSet.getBigDecimal("column_4")).isEqualByComparingTo("28");
        }
    }
}
