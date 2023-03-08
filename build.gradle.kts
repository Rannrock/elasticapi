plugins {
    id("java")
}


repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.spark:spark-core_2.13:3.3.2")
    implementation("org.apache.spark:spark-sql_2.13:3.3.2")

    implementation("co.elastic.clients:elasticsearch-java:8.6.2")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.12.3")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.1")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.8.1")
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}