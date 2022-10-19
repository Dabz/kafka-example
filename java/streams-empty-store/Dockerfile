#
# Build stage
#
FROM maven:3.8.6-openjdk-18 AS build
COPY src /home/app/src
COPY pom.xml /home/app
RUN mvn -f /home/app/pom.xml clean package

#
# Package stage
#
FROM openjdk:18
COPY --from=build /home/app/target/streams-empty-store-1.0-SNAPSHOT-jar-with-dependencies.jar /usr/local/lib/app.jar
EXPOSE 8080
ENTRYPOINT ["java","-cp", "/usr/local/lib/app.jar", "org.example.Stream"]
