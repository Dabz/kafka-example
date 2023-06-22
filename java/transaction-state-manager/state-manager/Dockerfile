# Build stage
#
FROM maven:3.6.0-jdk-11-slim AS build
COPY ./pom.xml /home/app/pom.xml
COPY ./avro /home/app/avro
COPY ./bom /home/app/bom
COPY ./injector /home/app/injector
COPY ./state-manager /home/app/state-manager
WORKDIR /home/app
RUN --mount=type=cache,target=/root/.m2 mvn clean package

#
# Package stage
#
FROM azul/zulu-openjdk-debian:17
WORKDIR /usr/src/project
VOLUME /config
COPY --from=build /home/app/state-manager/target/*-jar-with-dependencies.jar /usr/src/project/
COPY ./jmx_prometheus_javaagent-0.17.0.jar /usr/src/project/prom.jar
COPY ./exporter.yml /usr/src/project/
EXPOSE 8080
CMD sh -c "java  -javaagent:/usr/src/project/prom.jar=8080:/usr/src/project/exporter.yml -jar /usr/src/project/*.jar /config/kafka.properties"