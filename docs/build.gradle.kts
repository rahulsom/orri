import java.util.regex.Pattern

plugins {
    alias(libs.plugins.asciidoctor)
}

asciidoc {
    publications.named("main") {
        sourceSet {
            setSourceDir("src")
            sources("index.adoc")
            fatalWarnings(Pattern.compile(".*", Pattern.DOTALL))
        }
        output("asciidoctorj", "html")
    }
}

tasks.named("asciidoctorHtml") {
    inputs.files(
        fileTree("../orri-jdbc/src/test/java/io/github/rahulsom/orri") {
            include("DocumentationSamplesTest.java", "OrriDriverIntegrationTest.java")
        }
    ).withPathSensitivity(PathSensitivity.RELATIVE)
}
