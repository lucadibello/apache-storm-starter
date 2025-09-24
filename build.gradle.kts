plugins {
    // Apply the application plugin to add support for building a CLI application in Java.
    application
}

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
    maven(url = "https://repo.clojars.org/")
}

dependencies {
    // Use JUnit Jupiter for testing.
    testImplementation(libs.junit.jupiter)

    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    // This dependencies is used by the application.
    implementation(libs.storm)
    implementation(libs.fasterxml) // to read JSON jokes dataset
}

// Apply a specific Java toolchain to ease working on different environments.
java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

application {
    // Define the main class for the application.
    mainClass = "org.apache.storm.example.WordCountTopology"
}

sourceSets {
    val main by getting {
        resources.srcDirs("src/main/resources") // default resources directory
    }
}


tasks.named<Test>("test") {
    // Use JUnit Platform for unit tests.
    useJUnitPlatform()
}
