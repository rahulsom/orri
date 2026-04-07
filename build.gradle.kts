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

val dockerExecutable =
    providers.environmentVariable("DOCKER_BIN").orNull?.takeIf { it.isNotBlank() }
        ?: listOf("/usr/local/bin/docker", "/opt/homebrew/bin/docker", "/usr/bin/docker")
            .firstOrNull { candidate -> rootProject.file(candidate).canExecute() }
        ?: "docker"

tasks.register<Exec>("asciidoctorDocs") {
    description = "Generate Asciidoctor HTML docs in Docker"
    group = "documentation"
    notCompatibleWithConfigurationCache("Runs documentation generation in Docker via an external process.")
    commandLine(
        dockerExecutable,
        "run",
        "--rm",
        "-v",
        "${rootProject.projectDir.absolutePath}:/documents",
        "asciidoctor/docker-asciidoctor:main",
        "asciidoctor",
        "--destination-dir",
        "docs/build",
        "--backend=html5",
        "--failure-level",
        "WARN",
        "--out-file",
        "index.html",
        "docs/src/index.adoc",
    )
    inputs.file(layout.projectDirectory.file("docs/src/index.adoc"))
    outputs.file(layout.projectDirectory.file("docs/build/index.html"))
}
