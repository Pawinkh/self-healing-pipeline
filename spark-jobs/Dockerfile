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

# All three layer jobs live in this one image. docker-compose picks which
# script to run per service via `command:`.
COPY common.py bronze_job.py silver_job.py gold_job.py ./

# Overridden per-service in docker-compose.yml.
CMD ["python", "bronze_job.py"]
