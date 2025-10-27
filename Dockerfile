# ---------- build ----------
FROM eclipse-temurin:24-jdk AS build
WORKDIR /app

# gradle wrapper + зависимости
COPY gradlew build.gradle settings.gradle ./
COPY gradle gradle
RUN ./gradlew --no-daemon build -x test || return 0

# исходники и сборка
COPY src src
RUN ./gradlew --no-daemon clean build -x test

# ---------- runtime ----------
FROM eclipse-temurin:24-jre-alpine
WORKDIR /app

ENV JAVA_TOOL_OPTIONS="-XX:+UseContainerSupport -XX:+UseZGC -XX:+ZGenerational -XX:MaxRAMPercentage=75"

# копируем fat-jar (Spring Boot Gradle обычно кладёт в build/libs/)
COPY --from=build /app/build/libs/*.jar /app/app.jar

EXPOSE 8080

ENTRYPOINT ["java","-jar","/app/app.jar"]