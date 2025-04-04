FROM eclipse-temurin:17
WORKDIR /app

COPY target/syncdb-0.0.1-SNAPSHOT.jar app.jar

# Define o perfil "docker" ao iniciar
CMD ["java", "-jar", "app.jar", "--spring.profiles.active=docker"]
