FROM maven:3.9.11-eclipse-temurin-17 AS build
WORKDIR /source
COPY . .
RUN --mount=type=cache,target=/root/.m2,sharing=locked mvn -B -ntp -pl orders -am package -DskipTests
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /source/orders/target/orders-1.0.0.jar app.jar
COPY --from=build /source/orders/target/classes/practice/Probe.class practice/Probe.class
ENTRYPOINT ["java","-XX:MaxRAMPercentage=70","-jar","app.jar"]

