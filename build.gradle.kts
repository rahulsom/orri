import com.diffplug.gradle.spotless.SpotlessExtension

plugins {
    alias(libs.plugins.spotless) apply false
    alias(libs.plugins.waena.root)
}

allprojects {
    apply(plugin = "com.diffplug.spotless")

    group = "io.github.rahulsom"

    repositories {
        mavenCentral()
    }

    configure<SpotlessExtension> {
        java {
            palantirJavaFormat()
            target("src/**/*.java")
            targetExclude("build/**")
        }
        json {
            jackson()
            target("src/**/*.json", "*.json", ".vscode/**/*.json", ".sonarlint/**/*.json")
            targetExclude("build/**")
        }
        yaml {
            prettier()
            target("src/**/*.yaml", "*.yaml", "*.yml", ".github/**/*.yaml", ".github/**/*.yml")
            targetExclude("build/**")
        }
    }
}
