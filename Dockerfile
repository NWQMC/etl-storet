FROM maven:3.6.0-jdk-11 AS build
LABEL maintainer="gs-w_eto_eb_federal_employees@usgs.gov"

# Add pom.xml and install dependencies
COPY pom.xml /build/pom.xml
WORKDIR /build
RUN mvn -B dependency:go-offline

# Add source code and (by default) build the jar
COPY src /build/src
RUN mvn -B clean package


FROM usgswma/openjdk:debian-stretch-openjdk-11.0.3-731198dd265afe964d44718e66b840aa12769620

COPY --chown=1000:1000 --from=build /build/target/wqp-etl-storet-*.jar app.jar

USER $USER

CMD ["java", "-Xmx2G", "-server", "-jar", "app.jar"]
