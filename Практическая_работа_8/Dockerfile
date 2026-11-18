FROM maven:3.9.11-eclipse-temurin-17 AS build
WORKDIR /source
COPY . .
RUN --mount=type=cache,target=/root/.m2,sharing=locked mvn -B -ntp package -DskipTests
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /source/supplier/target/supplier-1.0.0.jar supplier.jar
COPY --from=build /source/orders/target/orders-1.0.0.jar orders.jar
COPY --from=build /source/delivery/target/delivery-1.0.0.jar delivery.jar
COPY --from=build /source/gateway/target/gateway-1.0.0.jar gateway.jar
COPY --from=build /source/gateway/target/classes/practice/Probe.class practice/Probe.class
ENTRYPOINT ["sh","-c","exec java -XX:MaxRAMPercentage=70 -jar /app/$APP.jar"]
