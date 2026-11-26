import com.diffplug.gradle.spotless.SpotlessExtension

buildscript {
    configurations.classpath {
        resolutionStrategy.eachDependency {
            if (requested.group == "org.eclipse.jgit") {
                useVersion("5.13.5.202508271544-r")
                because("JReleaser expects org.eclipse.jgit.lib.GpgObjectSigner, which is absent in JGit 7.x.")
            }
        }
    }
}

plugins {
    alias(libs.plugins.spotless) apply false
    alias(libs.plugins.waena.root)
}

allprojects {
    plugins.apply("com.diffplug.spotless")

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
