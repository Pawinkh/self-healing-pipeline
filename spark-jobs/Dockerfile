FROM python:3.11-slim

# PySpark needs a JVM. default-jdk-headless + the /usr/lib/jvm/default-java
# symlink is consistent across amd64/arm64, unlike hardcoding an
# arch-specific openjdk path.
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# All layer jobs and support modules live in this one image.
COPY *.py ./

# Overridden per-service in docker-compose.yml.
CMD ["python", "run_layers.py", "bronze", "silver1"]
