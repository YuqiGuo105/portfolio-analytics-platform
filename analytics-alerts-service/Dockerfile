# syntax=docker/dockerfile:1.6
FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /workspace

COPY pom.xml ./
COPY analytics-common/pom.xml analytics-common/pom.xml
COPY analytics-aggregator-service/pom.xml analytics-aggregator-service/pom.xml
COPY analytics-alerts-service/pom.xml analytics-alerts-service/pom.xml
RUN mvn -B -q -pl analytics-common,analytics-alerts-service -am -DskipTests dependency:go-offline

COPY analytics-common/src analytics-common/src
COPY analytics-alerts-service/src analytics-alerts-service/src
RUN mvn -B -q -pl analytics-alerts-service -am -DskipTests package

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app

RUN groupadd -r app && useradd -r -g app app
USER app

COPY --from=build /workspace/analytics-alerts-service/target/analytics-alerts-service.jar /app/app.jar

ENV JAVA_OPTS="-XX:MaxRAMPercentage=75.0 -XX:+UseG1GC"
ENV PORT=8094
EXPOSE 8094

ENTRYPOINT ["sh","-c","exec java $JAVA_OPTS -Dserver.port=${PORT} -jar /app/app.jar"]
