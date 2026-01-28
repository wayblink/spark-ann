# Spark-ANN API Server Dockerfile
# Multi-stage build for smaller final image

# Stage 1: Build
FROM sbtscala/scala-sbt:eclipse-temurin-jammy-11.0.22_7_1.9.8_2.12.18 AS builder

WORKDIR /app

# Copy build files first for better caching
COPY build.sbt .
COPY project/ project/

# Download dependencies
RUN sbt update

# Copy source code
COPY core/ core/
COPY api-server/ api-server/

# Build the assembly JAR
RUN sbt apiServer/assembly

# Stage 2: Runtime
FROM eclipse-temurin:11-jre-jammy

LABEL maintainer="Spark-ANN Team"
LABEL description="Spark-ANN API Server - Vector similarity search"

WORKDIR /app

# Create non-root user
RUN groupadd -r sparkann && useradd -r -g sparkann sparkann

# Copy the assembly JAR from builder
COPY --from=builder /app/api-server/target/scala-2.12/spark-ann-api-server-assembly.jar /app/spark-ann-api-server.jar

# Copy configuration
COPY api-server/src/main/resources/application.conf /app/application.conf

# Create directory for indexes
RUN mkdir -p /data/indexes && chown -R sparkann:sparkann /data

# Switch to non-root user
USER sparkann

# Expose API port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:8080/api/v1/health || exit 1

# Environment variables
ENV JAVA_OPTS="-Xmx2g -Xms512m"
ENV ANN_HOST="0.0.0.0"
ENV ANN_PORT="8080"

# Run the server
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/spark-ann-api-server.jar"]
