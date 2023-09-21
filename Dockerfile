# Stage 1: Build
FROM maven:3.9-amazoncorretto-21 AS builder

WORKDIR /app

# Copy pom.xml first for dependency caching
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Copy source and build
COPY src ./src
RUN mvn package -DskipTests -B

# Stage 2: Runtime
FROM amazoncorretto:21-alpine

WORKDIR /app

# Copy the built jar
COPY --from=builder /app/target/*.jar app.jar

# Default to backup mode, override with SPRING_PROFILES_ACTIVE=web for web interface
ENV SPRING_PROFILES_ACTIVE=backup
ENV JAVA_OPTS="-Xms512m -Xmx3g"

EXPOSE 8080

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
