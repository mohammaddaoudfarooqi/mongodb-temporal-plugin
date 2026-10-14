# Kubernetes

Running `temporal-mongodb-server` on Kubernetes.

## v1.0 position

**First-class Kubernetes manifests are NOT shipped in v1.0.** The
plugin is a standard Go binary that reads a YAML config; it runs
fine in any container orchestrator with standard patterns.

This page captures the recommended patterns so operators don't
re-discover them. A community-maintained Helm chart may follow
post-GA.

## Container image

Build from the supplied Dockerfile (or the one in the consumer
binary repo). The binary is statically linked on Linux/amd64 and
Linux/arm64; no system packages required at runtime.

```dockerfile
FROM gcr.io/distroless/static:nonroot
COPY bin/temporal-mongodb-server /usr/local/bin/
USER nonroot
ENTRYPOINT ["/usr/local/bin/temporal-mongodb-server"]
CMD ["--config", "/etc/temporal/config.yaml"]
```

## Deployment shape

- **Deployment**, not StatefulSet. Temporal is the stateful layer;
  the plugin is a stateless adapter.
- Replicas: 3-5 typical for mid-tier; scale with `temporal-server`'s
  history + matching services.
- Resource requests/limits: start with 500m CPU, 512Mi memory per
  replica; profile under load.

## Secrets

Mount the MongoDB credential as a projected env var or file. Prefer
files + `passwordCommand` for large credentials.

```yaml
env:
  - name: MONGO_TEMPORAL_PW
    valueFrom:
      secretKeyRef:
        name: mongo-temporal-credential
        key: password
```

And in the plugin config:

```yaml
customDatastore:
  options:
    uri: mongodb+srv://cluster0.example.mongodb.net/
    username: temporal_runtime
    passwordEnv: MONGO_TEMPORAL_PW
```

## ConfigMap

Mount the Temporal config YAML as a ConfigMap; the plugin's
options live inside it under `persistence.datastores.*`.

## Probes

- **Liveness**: HTTP probe on Temporal's membership port (7233 by
  default). The plugin itself does not expose an HTTP endpoint.
- **Readiness**: same — Temporal reports ready when persistence is
  reachable.

## Network

- Egress to MongoDB Atlas: allow the VPC-peered or PrivateLink
  subnet CIDR.
- Self-hosted: allow egress to 27017 on the MongoDB replica set
  members.
- TLS: handled by the driver using the in-pod CA bundle. For
  custom CAs, mount the bundle and set `tlsCAFile`.

## Schema management

Run `temporal-mongodb-tool setup-schema` as a Kubernetes Job before
rolling out Temporal. Example:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: temporal-schema-setup
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: setup
          image: your-registry/temporal-mongodb-tool:0.1.0
          args:
            - setup-schema
            - --config
            - /etc/temporal/config.yaml
            - --num-history-shards
            - "512"
          env:
            - name: MONGO_TEMPORAL_ADMIN_PW
              valueFrom:
                secretKeyRef:
                  name: mongo-temporal-admin-credential
                  key: password
          volumeMounts:
            - name: config
              mountPath: /etc/temporal
      volumes:
        - name: config
          configMap:
            name: temporal-config
```

The Job runs with the **admin** credential (elevated); runtime
pods use the least-privilege runtime credential.

## Upgrade flow

1. Run `update-schema` as a Job (preStop hook or manual trigger).
2. Rolling-update the `temporal-mongodb-server` Deployment.
3. Confirm via `kubectl logs` that each pod reports
   "Schema version check passed".

## Horizontal Pod Autoscaler

Use Temporal's published metrics (`temporal_persistence_latency`)
or the plugin's `persistence_mongo_op_latency_ms` as HPA signals.
Do NOT scale on CPU alone; the plugin is IO-bound.
