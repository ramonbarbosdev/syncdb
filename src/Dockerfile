# Etapa 1: build
FROM maven:3.9.6-eclipse-temurin-17 AS build

WORKDIR /app

# Copia apenas os arquivos necessários para cache eficiente
COPY pom.xml .
RUN mvn dependency:go-offline

COPY . .

RUN mvn clean package -DskipTests

# Etapa 2: imagem final
FROM eclipse-temurin:17-jdk-alpine

WORKDIR /app

EXPOSE 8080

COPY --from=build /app/target/syncdb-0.0.1-SNAPSHOT.jar app.jar

ENTRYPOINT ["java", "-jar", "app.jar"]
