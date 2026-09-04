# syntax=docker/dockerfile:1.7
# =====================================================================
#  E-Commerce Kafka Streams analytics — production container image
#
#  Three stages:
#    1. build    — JDK 21, Gradle wrapper, produces the Spring Boot fat jar
#    2. extract  — explodes the LAYERED jar into its four layers
#    3. runtime  — slim JRE 21, non-root, layers copied most-stable-first
#
#  WHY LAYERS (and why no build.gradle.kts change was needed):
#    Spring Boot >= 2.4 builds a layered jar by DEFAULT (verified: the jar carries
#    BOOT-INF/layers.idx and `java -Djarmode=layertools -jar app.jar list` prints
#    dependencies / spring-boot-loader / snapshot-dependencies / application).
#    So `layertools extract` works with the build file exactly as another agent owns it.
#    Copying the four layers as four COPY instructions — dependencies first, the ~100 KB
#    of application classes last — means a code-only change re-pushes kilobytes instead
#    of the ~98 MB fat jar.
#
#  WHY A SEPARATE `extract` STAGE:
#    layertools needs a JVM but the extracted output is plain files. Doing it in its own
#    stage keeps the JDK, the Gradle caches and the intermediate fat jar out of the final
#    image entirely.
# =====================================================================


# ─────────────────────────────────────────────────────────────────────
#  Stage 1 — build
# ─────────────────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jdk-jammy AS build

WORKDIR /workspace

# Wrapper + build scripts FIRST. These change far less often than src/, so the
# expensive "download Gradle, resolve the dependency graph" layer stays cached
# across ordinary code changes.
COPY gradlew gradlew.bat settings.gradle.kts build.gradle.kts gradle.properties ./
COPY gradle gradle
RUN chmod +x ./gradlew && ./gradlew --no-daemon --version

# Warm the dependency cache without the sources. `dependencies` resolves the compile and
# runtime classpaths; it is allowed to fail (|| true) so that a network hiccup degrades to
# "slower build", not "broken build" — the real resolution happens in the bootJar below.
RUN ./gradlew --no-daemon -q dependencies --configuration runtimeClasspath > /dev/null 2>&1 || true

COPY src src

# -x test: unit tests run in CI (.github/workflows/ci.yml) against the real Gradle cache,
# not inside the image build. Running them here would double the CI time and give the image
# build a reason to fail for a non-image reason.
RUN ./gradlew --no-daemon bootJar -x test


# ─────────────────────────────────────────────────────────────────────
#  Stage 2 — explode the layered jar
# ─────────────────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jdk-jammy AS extract

WORKDIR /extract
COPY --from=build /workspace/build/libs/*.jar application.jar
# mkdir -p after the extract: snapshot-dependencies is empty for this project (no SNAPSHOT
# deps), so creating every layer directory unconditionally keeps the COPY instructions in the
# runtime stage from failing on a missing path if the dependency set ever changes.
RUN java -Djarmode=layertools -jar application.jar extract --destination /extract \
    && mkdir -p dependencies spring-boot-loader snapshot-dependencies application \
    && rm -f application.jar


# ─────────────────────────────────────────────────────────────────────
#  Stage 3 — runtime
# ─────────────────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jre-jammy AS runtime

# curl is installed deliberately, not incidentally: the Kubernetes readiness probe is an
# `exec` probe that reads GET /api/analytics/streams/status and greps for "queryable":true.
# It has to be an exec probe because that endpoint answers 200 for EVERY stream state by
# design (see AnalyticsController), so an httpGet probe against it would call a REBALANCING
# pod healthy. See k8s/README.md, "Readiness".
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl; \
    rm -rf /var/lib/apt/lists/*; \
    groupadd --system --gid 10001 app; \
    useradd --system --uid 10001 --gid 10001 --home-dir /app --shell /usr/sbin/nologin app; \
    mkdir -p /app /var/lib/kafka-streams; \
    chown -R 10001:10001 /app /var/lib/kafka-streams

WORKDIR /app

# Layers copied most-stable → least-stable. Only the last one changes on a code edit.
COPY --from=extract --chown=10001:10001 /extract/dependencies/          ./
COPY --from=extract --chown=10001:10001 /extract/spring-boot-loader/    ./
COPY --from=extract --chown=10001:10001 /extract/snapshot-dependencies/ ./
COPY --from=extract --chown=10001:10001 /extract/application/           ./

# NON-ROOT. Numeric so Kubernetes `runAsUser: 10001` / `runAsNonRoot: true` can be enforced
# without the kubelet having to resolve a name out of /etc/passwd.
USER 10001:10001

# The image is designed to run with a READ-ONLY root filesystem. Exactly two paths must be
# writable and both are mounted from outside:
#   /var/lib/kafka-streams  — RocksDB state (PVC in k8s, named volume in compose)
#   /tmp                    — JVM scratch, Tomcat temp dir (emptyDir in k8s)
VOLUME ["/var/lib/kafka-streams"]

EXPOSE 8090

# JAVA_TOOL_OPTIONS notes:
#   MaxRAMPercentage, not -Xmx — the heap then tracks the CONTAINER limit (the JVM is
#   cgroup-aware since JDK 10) instead of a number baked in here that silently disagrees
#   with the Kubernetes memory limit.
#
#   60 %, not the usual 75 % — this is a RocksDB application. Every state store keeps its
#   block cache, memtables and index/filter blocks OFF-heap. A 75 % heap on a 2 Gi limit
#   leaves ~500 Mi for RocksDB + metaspace + thread stacks + code cache, and the pod gets
#   OOMKilled by the kernel while the heap graph looks perfectly healthy.
#
#   ExitOnOutOfMemoryError — a Streams instance that has OOMed cannot make progress. Dying
#   lets Kubernetes restart it and lets the group rebalance; limping along holds the
#   partitions hostage.
ENV APP_STREAMS_STATE_DIR=/var/lib/kafka-streams \
    SERVER_PORT=8090 \
    JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=60.0 -XX:InitialRAMPercentage=40.0 -XX:+ExitOnOutOfMemoryError -XX:+UseG1GC -Djava.io.tmpdir=/tmp"

# JarLauncher directly rather than `java -jar`: the jar has already been exploded, so there is
# nothing to launch with -jar. Exec form (no shell) so the JVM is PID 1 and receives SIGTERM
# straight from the container runtime — which is what makes graceful shutdown work at all.
ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]
