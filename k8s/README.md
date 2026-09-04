# Kubernetes deployment — `ecommerce-streams`

Deploys the Spring Boot + Kafka Streams analytics application as a **StatefulSet** with
per-pod persistent RocksDB state and working multi-instance Interactive Queries.

> **Status of this directory.** These manifests were authored and validated statically only.
> There was no Kubernetes cluster and no Docker daemon available on the authoring machine, so
> nothing here has been applied to a live cluster, and the image has never been built. See
> [What was and was not verified](#what-was-and-was-not-verified) at the bottom — read it
> before you trust anything above it.

---

## 1. Why a StatefulSet and not a Deployment

This is the single decision that shapes every other file here, so it is worth stating plainly.

The application materialises five state stores in **RocksDB on local disk** under
`app.streams.state-dir`. That directory is not a cache — it is a materialised view of the
changelog topics, and the `customer-spending` store in particular holds **lifetime** running
totals with no window to age them out.

A `Deployment` gives each replacement pod an empty filesystem. Every restart, reschedule and
rolling update would therefore trigger a **full changelog replay** before the instance could
serve a single query, and the length of that replay grows monotonically over the life of the
application. A `StatefulSet` with `volumeClaimTemplates` binds pod `ecommerce-streams-N` to
PVC `state-ecommerce-streams-N` permanently, so a restarted pod re-attaches its own RocksDB
tree and replays only the delta.

There is a second, independent reason: Interactive Queries need each instance to advertise a
**stable, peer-reachable address** (§2). A `Deployment`'s pod names are random, so the
advertised address would change on every restart — and because `KafkaStreamsConfig.java`
derives the state directory leaf *from that address*, so would the on-disk path, defeating the
first reason even if the volume somehow survived.

---

## 2. The `application-server` wiring (read this one)

`app.streams.application-server` is the address each instance publishes into the consumer-group
metadata. It is the **sole discovery mechanism** behind cross-instance queries: without it,
`queryMetadataForKey()` and `streamsMetadataForStore()` return `HostInfo("unavailable", -1)`
and there is literally no way for one instance to find another.

The application defaults it to `localhost:${server.port}`. That default is correct for two JVMs
on one laptop and **catastrophic in Kubernetes**: inside a pod, `localhost` resolves to *that
pod*. Every instance would advertise `localhost:8090`, every instance would read that as its
own loopback, and the fan-out would query itself N times — returning one shard's data N times
over, with HTTP 200 and nothing to indicate anything was missing. That is exactly the
silent-partial-results class of bug that commit `7a52ef7` exists to eliminate, reintroduced
through deployment configuration.

`30-statefulset.yaml` composes the real value from the downward API:

```yaml
- name: POD_NAME
  valueFrom: { fieldRef: { fieldPath: metadata.name } }
- name: POD_NAMESPACE
  valueFrom: { fieldRef: { fieldPath: metadata.namespace } }
- name: HEADLESS_SERVICE
  value: "ecommerce-streams-headless"
- name: APP_STREAMS_APPLICATION_SERVER
  value: "$(POD_NAME).$(HEADLESS_SERVICE).$(POD_NAMESPACE).svc.cluster.local:8090"
```

which resolves per pod to e.g.

```
ecommerce-streams-0.ecommerce-streams-headless.kafka-analytics.svc.cluster.local:8090
```

Three details that are easy to get wrong:

| Detail | Why |
|---|---|
| **DNS name, not `$(POD_IP)`** | The pod IP changes on every restart. Peers would hold a stale IP in group metadata until the next rebalance — and the RocksDB directory name (derived from this string) would change on every restart, orphaning the restored state on the PVC. |
| **`publishNotReadyAddresses: true`** on the headless Service | Readiness means "Streams is RUNNING". A pod that is rebalancing is *not ready* — but its peers must still be able to resolve its DNS name, because that name is what it published into the group metadata. Without this flag, every rolling restart makes the surviving pods report `partial: true` for the whole restart window. |
| **`$(VAR)` ordering** | Kubernetes expands `$(VAR)` only against variables declared **earlier in the same `env` list**. `HEADLESS_SERVICE` is declared before it is used. |

### Consequence for the state directory

`KafkaStreamsConfig.java` builds the state path as `stateDirBase + "/" + host + "-" + port`.
With the value above, the real RocksDB tree lives at:

```
/var/lib/kafka-streams/ecommerce-streams-0.ecommerce-streams-headless.kafka-analytics.svc.cluster.local-8090
```

This is stable for a given pod (StatefulSet pod names are stable), which is what makes the PVC
useful. **But it means renaming the namespace or the headless Service silently invalidates
every instance's state and forces a full restore.** Pick both names once.

---

## 3. Apply order

The order matters in two places: the namespace must exist before anything in it, and both
Services should exist before the StatefulSet so that pod DNS resolves on the pods' first
attempt rather than after a DNS retry.

```sh
# 0. Set the image to a real, immutable tag first — the manifest ships with a placeholder.
#    CI publishes ghcr.io/<owner>/kafka-streaming-analytics:<full-git-sha>.
$EDITOR k8s/30-statefulset.yaml     # replace  :REPLACE_WITH_GIT_SHA

kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/10-secret.yaml          # placeholder — see the warning inside it
kubectl apply -f k8s/11-configmap.yaml       # point SPRING_KAFKA_BOOTSTRAP_SERVERS at your cluster
kubectl apply -f k8s/20-service-headless.yaml
kubectl apply -f k8s/21-service.yaml
kubectl apply -f k8s/30-statefulset.yaml
kubectl apply -f k8s/40-poddisruptionbudget.yaml
kubectl apply -f k8s/41-hpa.yaml             # requires metrics-server; read its caveats
```

The filenames are numerically ordered, so `kubectl apply -f k8s/` applies them in this order
too — but `kubectl` does not wait between objects, so on a cold cluster prefer the explicit
sequence above (or `kubectl apply -f k8s/ && kubectl rollout status ...`).

### Prerequisites

* A Kafka cluster reachable from the namespace. Update `SPRING_KAFKA_BOOTSTRAP_SERVERS` and
  `SPRING_KAFKA_STREAMS_BOOTSTRAP_SERVERS` in `11-configmap.yaml`.
* The application topics must exist with **3 partitions and RF ≥ 3**
  (`docker/kafka-init.sh` is the reference), and the brokers must run
  `min.insync.replicas=2` and a replicated `__transaction_state` — `EXACTLY_ONCE_V2` depends
  on it.
* A default StorageClass. Pin an **SSD-backed** class in `volumeClaimTemplates` for anything
  real: changelog restore and RocksDB compaction are I/O bound.
* `metrics-server`, for the HPA only.

---

## 4. Verify

```sh
NS=kafka-analytics

# Rollout finished, both pods Ready. "Ready" here means Kafka Streams reached RUNNING,
# not merely "the JVM started" — see §5.
kubectl -n $NS rollout status statefulset/ecommerce-streams --timeout=10m
kubectl -n $NS get pods -l app.kubernetes.io/name=ecommerce-streams -o wide

# Each pod bound its OWN PVC (this is the StatefulSet doing its job).
kubectl -n $NS get pvc -l app.kubernetes.io/component=streams-state
#  NAME                          STATUS   VOLUME   CAPACITY
#  state-ecommerce-streams-0     Bound    pvc-...  10Gi
#  state-ecommerce-streams-1     Bound    pvc-...  10Gi
```

### The check that actually matters: do the instances see each other?

```sh
kubectl -n $NS port-forward svc/ecommerce-streams 8090:80 &
curl -s localhost:8090/api/analytics/streams/status | jq
```

Look for **all three** of these:

```jsonc
{
  "state": "RUNNING",
  "queryable": true,
  // NOT "localhost:8090" — if you see localhost here, the downward-API wiring in §2
  // did not take effect and every cross-instance query is silently querying itself.
  "localHost": "ecommerce-streams-0.ecommerce-streams-headless.kafka-analytics.svc.cluster.local:8090",
  "instances": [
    // BOTH pods must appear. One entry with two pods running means peer discovery failed.
    { "host": "ecommerce-streams-0....:8090", "self": true,  "topicPartitions": ["orders-0", "orders-2"] },
    { "host": "ecommerce-streams-1....:8090", "self": false, "topicPartitions": ["orders-1"] }
  ]
}
```

The partition lists must be **disjoint and together cover 0–2**. If one pod claims all three,
the other has not joined the group.

### And that the fan-out actually merges

```sh
# Cluster-wide (fans out to peers and merges). partial must be false, failures empty.
curl -s "localhost:8090/api/analytics/customer-spending" | jq '{partial, failures, n: (.customers|length)}'

# This instance's shard only.
curl -s "localhost:8090/api/analytics/customer-spending?local=true" | jq '{n: (.customers|length)}'
```

The `local=true` count should be **strictly smaller** than the cluster-wide count. If they are
equal with two pods running, the fan-out is not reaching the peer — check `localHost` above.

### Resilience spot-checks

```sh
# Graceful shutdown: delete a pod and watch the survivor. It should briefly report
# partial:true, then return to a complete answer once the rebalance settles.
kubectl -n $NS delete pod ecommerce-streams-1
watch -n2 'curl -s localhost:8090/api/analytics/customer-spending | jq "{partial, failures}"'

# State really is persistent: after the pod comes back, its logs should show a SHORT restore,
# not a replay from offset 0.
kubectl -n $NS logs ecommerce-streams-1 | grep -i "restor"

# The PDB is protecting you: ALLOWED DISRUPTIONS should be 1 when both pods are healthy.
kubectl -n $NS get pdb ecommerce-streams
```

---

## 5. Probes — and the one thing that is deliberately interim

| Probe | Endpoint | Rationale |
|---|---|---|
| **startup** | `httpGet /actuator/health/liveness`, 30 × 10 s | Gates the other two. A cold pod must restore state before it is useful, and how long that takes depends on history volume, not on anything predictable. Raise `failureThreshold` for a large restore, never `periodSeconds`. |
| **liveness** | `httpGet /actuator/health/liveness` | Deliberately the *narrowest* check — Spring's `livenessState`, i.e. "the context is running". **It must not depend on Kafka.** If it did, a broker outage would restart-loop every pod, throwing away restored RocksDB state and guaranteeing recovery takes longer than the outage. |
| **readiness** | `exec` → `curl … /api/analytics/streams/status \| grep -q '"queryable":true'` | Must reflect the **stream** state. See below. |

### Why readiness is an `exec` probe, and what should replace it

Two endpoints were candidates and both fail as a plain `httpGet`:

* **`/actuator/health/readiness`** currently resolves to Spring's `readinessState` alone, which
  flips to `ACCEPTING_TRAFFIC` as soon as the application context starts. It knows nothing
  about Kafka Streams, so it would mark a `REBALANCING` pod ready.
* **`/api/analytics/streams/status`** *does* know the stream state — but it answers **200 for
  every state by explicit design** (it is the endpoint you call to find out *why* the query
  endpoints are 503-ing, so it must never 503 itself). An `httpGet` probe only inspects the
  status code, so it would *also* mark a `REBALANCING` pod ready.

So the probe reads the **body**. `queryable` is `true` if and only if
`KafkaStreams.State == RUNNING`, which is precisely the condition under which the pod can serve
Interactive Queries. A pod that is `CREATED`, `REBALANCING`, `PENDING_SHUTDOWN` or `ERROR` is
removed from the client Service's endpoints, while the headless Service keeps it resolvable for
its peers.

**This works today with zero application changes, and it is still the second-best answer.**
The right one is a custom health indicator — see item 2 in §7.

---

## 6. Graceful shutdown

A hard kill mid-rebalance strands the instance's tasks until the group's session timeout
expires. Until then **nobody** owns those partitions: the survivors keep answering
`partial: true` and the dead instance's partitions make no progress at all. Three settings
work together:

1. `SERVER_SHUTDOWN=graceful` + `SPRING_LIFECYCLE_TIMEOUT_PER_SHUTDOWN_PHASE=45s`
   (`11-configmap.yaml`) — Tomcat stops accepting new connections and lets in-flight requests
   (including fan-out RPCs) finish.
2. `preStop: sleep 15` — endpoint removal is **asynchronous** and races SIGTERM. Without the
   pause, the pod stops accepting connections while traffic is still being routed to it, and
   each such request surfaces as a fan-out *failure* on a sibling rather than a clean redirect.
3. `terminationGracePeriodSeconds: 120` — 15 s drain + up to 45 s Spring shutdown +
   `KafkaStreams.close()`, which must commit the open EOS transaction, flush RocksDB and leave
   the consumer group cleanly. The default 30 s is not enough for that; the extra 90 s costs
   nothing and getting it wrong stalls every deploy.

---

## 7. Handed back to the application owners

`src/`, `build.gradle.kts`, `README.md` and `docs/` are owned by other work in flight, so the
following are **reported, not made**. Each is specific enough to apply directly.

### 1. `build.gradle.kts` — add the Prometheus registry (**required** for observability)

```kotlin
runtimeOnly("io.micrometer:micrometer-registry-prometheus")
```

*Why:* `spring-boot-starter-actuator` ships Micrometer's **core**, but a registry is what
actually renders metrics in a scrapeable format. Without this dependency
`/actuator/prometheus` returns **404**, and everything downstream of it is dead: the
`ecommerce-streams` job in `monitoring/prometheus.yml` shows DOWN, the Grafana panels for
stream state / process rate / commit latency / JVM stay blank, and the `KafkaStreamsNotRunning`
alert never fires because its series does not exist.

Consumer-lag panels and lag alerts do **not** depend on this — they come from `kafka-exporter`,
which reads the brokers directly. That split is deliberate: the most important metric keeps
working even when the application is down.

Once added, no config change is needed — `MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE` already
lists `prometheus` in `11-configmap.yaml` and in `docker-compose.yml`.

Optional but recommended alongside it, so every series is attributable to a pod:

```yaml
management:
  metrics:
    tags:
      application: ${spring.application.name}
```

### 2. A `KafkaStreams` health indicator (**recommended** — replaces the exec readiness probe)

*Why:* it turns readiness into a first-class `httpGet` probe, removes the need for `curl` in
the runtime image (smaller image, smaller attack surface), and makes the stream state visible
at `/actuator/health` for humans and dashboards instead of only to the kubelet.

Sketch (a new `@Component`; **no Java was written by this task**):

```java
// src/main/java/com/ecommerce/streaming/health/KafkaStreamsHealthIndicator.java
@Component("kafkaStreams")
@RequiredArgsConstructor
public class KafkaStreamsHealthIndicator implements HealthIndicator {

    private final StreamsBuilderFactoryBean factoryBean;

    @Override
    public Health health() {
        KafkaStreams streams = factoryBean.getKafkaStreams();
        if (streams == null) {
            return Health.down().withDetail("state", "NOT_INITIALIZED").build();
        }
        KafkaStreams.State state = streams.state();
        // RUNNING only. REBALANCING is explicitly NOT healthy for readiness purposes:
        // the stores are not queryable and the REST API answers 503.
        Health.Builder b = (state == KafkaStreams.State.RUNNING) ? Health.up() : Health.outOfService();
        return b.withDetail("state", state.name())
                .withDetail("threads", streams.metadataForLocalThreads().size())
                .build();
    }
}
```

Then, in this directory, `11-configmap.yaml` gains:

```yaml
MANAGEMENT_ENDPOINT_HEALTH_GROUP_READINESS_INCLUDE: "readinessState,kafkaStreams"
```

and `30-statefulset.yaml`'s readiness probe becomes:

```yaml
readinessProbe:
  httpGet:
    path: /actuator/health/readiness
    port: http
```

> ⚠️ **Do not add that ConfigMap key before the bean exists.** Spring Boot validates health
> group membership at startup and **fails the application** if a group names a contributor that
> is not registered. That is why the key is currently commented out rather than shipped.
> Leave the liveness group alone — it must stay Kafka-independent (§5).

### 3. Kafka security is not wireable from config alone (**blocker for a secured cluster**)

`10-secret.yaml` is a placeholder and **cannot** be made to work by populating it, because
`KafkaStreamsConfig#kStreamsConfig` builds its property map from `new HashMap<>()` and sets
every entry by hand. It never reads `spring.kafka.properties.*` or
`spring.kafka.streams.properties.*`, so any `SASL`/`SSL` setting injected through the
environment is **silently ignored by the Streams client** — the client that matters.

The change: have that method merge in the Spring-bound Kafka properties before returning, e.g.
inject `KafkaProperties` and seed the map with `kafkaProperties.buildStreamsProperties(null)`
(Boot 3.2 signature), or explicitly read a `spring.kafka.streams.properties.*` map and
`props.putAll(...)` it. Spring's own producer/consumer (used by `OrderProducer` /
`ProductProducer`) would pick the settings up without any change; only the Streams client
needs this.

Until then, deploy only onto a `PLAINTEXT` broker listener reachable from the namespace, and
say so out loud — the Secret's presence must not be mistaken for authenticated traffic.

### 4. Note for whoever owns `README.md` / `docs/`

The top-level README describes running the app on the host with `./gradlew bootRun` against a
compose stack that does not include it. That is no longer the only way, and the new path is
worth documenting:

* `docker compose up -d` now brings up the **entire** stack including **two** application
  instances (`app-1` on `:8090`, `app-2` on `:8091`) with distinct
  `app.streams.application-server` values — so multi-instance Interactive Queries, the
  `partial`/`failures` fields and the RPC fan-out can be exercised with one command instead of
  by hand-starting a second JVM.
* New endpoints on the host: Prometheus `:9091` (not `:9090` — `kafka-ui` already has it),
  Grafana `:3000` (anonymous viewer, admin/admin), kafka-exporter `:9308`.
* Total compose memory footprint is now **≈ 8.4 GB**, up from ≈ 5.4 GB. On macOS/Windows this
  is bounded by the Docker Desktop VM allocation, which commonly defaults to 8 GB — that needs
  raising to ~12 GB, or the two app containers should be left out and the app run on the host
  as before.
* `Dockerfile`, `k8s/`, `monitoring/` and `.github/workflows/ci.yml` are new and should be
  listed in the project layout section.
* The image is published only with **immutable git-SHA tags**; there is no `:latest`.

---

## 8. Teardown

```sh
kubectl delete -f k8s/
```

**This leaves the PVCs behind**, by design (`persistentVolumeClaimRetentionPolicy` is
deliberately unset). Scaling 3 → 2 → 3 then re-attaches existing state instead of forcing a
full restore. To reclaim the storage, and accept a full changelog replay on next deploy:

```sh
kubectl -n kafka-analytics delete pvc -l app.kubernetes.io/component=streams-state
kubectl delete namespace kafka-analytics
```

---

## What was and was not verified

**Verified**

* `./gradlew bootJar` succeeds under JDK 21 and produces a **layered** jar — confirmed with
  `java -Djarmode=layertools -jar … list` (`dependencies`, `spring-boot-loader`,
  `snapshot-dependencies`, `application`). No `build.gradle.kts` change was needed for the
  Dockerfile's layer caching.
* The jar manifest's `Main-Class` is `org.springframework.boot.loader.launch.JarLauncher`
  (the Boot 3.2 package, *not* the pre-3.2 `org.springframework.boot.loader.JarLauncher`) —
  this is what the Dockerfile's `ENTRYPOINT` invokes.
* `docker compose config -q` parses the extended `docker-compose.yml` cleanly, and the
  resolved model was inspected: 15 services, 4 named volumes, `kafka-1`'s environment
  unchanged, `app-1`/`app-2` carrying distinct `APP_STREAMS_APPLICATION_SERVER` values.
* `git diff docker-compose.yml` shows **234 insertions, 0 deletions** — the additions are
  provably additive.
* Every YAML file in `k8s/` and `monitoring/`, and `.github/workflows/ci.yml`, parses as valid
  YAML; the Grafana dashboard parses as valid JSON. Each manifest's `apiVersion`/`kind`/
  `metadata` were inspected.
* Property-name mapping was checked against the actual `@Value` expressions in
  `KafkaStreamsConfig.java`, `InteractiveQueryClientConfig.java` and
  `InteractiveQueryService.java`, so every `ConfigMap` key overrides a property that really
  exists (`app.streams.rpc.fan-out-timeout-ms` → `APP_STREAMS_RPC_FAN_OUT_TIMEOUT_MS`, etc.).

**NOT verified — no Docker daemon and no Kubernetes cluster were available**

* **The image has never been built.** The Dockerfile is unexecuted. Stage ordering, the
  `layertools --destination` invocation, the `apt-get install curl` step and the non-root
  `COPY --chown` layout are all reasoned-through, not run.
* **No manifest was applied, and not even schema-validated.** `kubectl` v1.36 is installed but
  contacts the API server even for `--dry-run=client --validate=false`, and there is no
  cluster — so the validation here is YAML well-formedness plus review, **not** schema
  conformance. Run `kubectl apply --dry-run=server -f k8s/` against a real cluster (or
  `kubeconform`) before trusting field names.
* **Probe behaviour is untested.** In particular the readiness `exec` command depends on
  `curl` being present in the image and on the JSON containing the exact substring
  `"queryable":true` (no space after the colon). Jackson's default output has no spaces, and
  `StreamsStatusResponse` is a plain Lombok `@Data` bean, so this holds — but it has not been
  observed. Confirm with
  `kubectl exec … -- curl -s localhost:8090/api/analytics/streams/status` on first deploy.
* **The Prometheus and Grafana stack has never been started**, and no metric name was observed
  from a live instance. The `kafka_consumergroup_lag` / `kafka_topic_partition_*` names come
  from `kafka-exporter`'s documented output and are reliable; the `kafka_stream_*` names depend
  on Micrometer's Kafka Streams binding and, more fundamentally, on the missing
  `micrometer-registry-prometheus` dependency (§7.1). **Expect to adjust the queries in
  `monitoring/grafana/dashboards/kafka-streams-analytics.json` against `/actuator/prometheus`
  output once that dependency is added.**
* **The CI workflow has never run.** It is YAML-valid and the action versions are pinned, but
  nothing about the GHCR push, the Trivy gate or the Testcontainers job has been exercised. The
  `integration-test` job calls `./gradlew integrationTest`, a task another agent added
  concurrently; at the time of writing no `@Tag("integration")` test exists yet, so that job
  would pass vacuously.
