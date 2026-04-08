plugins {
    `java-library`
    id("com.github.rahulsom.waena.published")
}

description = "JDBC driver for querying and modifying Google Sheets as tables and views."

dependencies {
    implementation(libs.google.api.client.gson)
    implementation(libs.google.auth.http)
    implementation(libs.google.sheets)
    implementation(libs.h2)
    testImplementation(libs.assertj)
    testImplementation(libs.google.oauth.client.jetty)
}

testing {
    suites {
        val test by getting(JvmTestSuite::class) {
            useJUnitJupiter(libs.versions.junit.get())
            targets.all {
                testTask.configure {
                    listOf(
                            "orri.integration",
                            "orri.integration.spreadsheetId"
                    ).forEach { propertyName ->
                        System.getProperty(propertyName)?.let { value ->
                            systemProperty(propertyName, value)
                        }
                    }
                }
            }
        }
    }
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}
