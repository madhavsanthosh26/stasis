# STASIS
    The word stasis means reaching a state where the opposing force cancell out each other where things stop moving. This software detect failure in any modules of a software and restarting them thus preventing big failure.

## PURPOSE
    In a multi mircroservice application the many service are running one single failure could affect the whole system so we need an intelligent vigilante who detects failure and instantly restart the service thus preventing the big failure.
    We have a multi microservice application if any one service fails our ingestion plane detect it and notifies our orchestration plane which then takes the appropriate step needed to resume normal operation and add it to the log.
    Instead of simply finding the cause we take the needed meassure to solve it which makes us different.

## ARCHITECTURAL DIAGRAM.
Online Boutique (11 services) → Ingestion Plane watches pod health every 10s
→ publishes SignalEvent to Kafka (raw-signals topic)
→ Orchestration Plane consumes signal, detects errorRate > 0.7
→ restarts failing pod via Kubernetes API
→ writes to audit.log
→ 60s cooldown prevents remediation storms

## TECH STACK
### BACKEND
    - JAVA 21
    - SpringBoot 4.1
    - Apache Kafka - message bus between planes
    - Redis - cooldown and state management
    - Kubernetes - infrasturcture for microservices
    - fabric8 kubernetes-client - Java library to control Kubernetes
    - Docker - local infrastructure management
    - Google Online Boutique - demo microservice playground
    

## MODULES
    - INGESTION PLANE:- every 10seconds the Ingestion-plane checks the kubernetes pods and if pod isn't ready it send signal to Kafka
    - ORCHESTRATION PLANE:- listens to Kafka and when it detects an error rate above 0.7, it restart the pod, writes to the audit log and enforces a 60second cooldown to prevent remedition storms.
    - DEPLOYMENT LAYER:- runs of Google Online Botique.

## How to run

**Terminal 1 — start infrastructure:**
```bash
cd deploy/
docker compose up -d
minikube start --driver=docker
kubectl get pods
```

**Terminal 2 — start ingestion plane:**
```bash
cd ingestion-plane/
mvn spring-boot:run
```

**Terminal 3 — start orchestration plane:**
```bash
cd orchestration-plane/
mvn spring-boot:run
```

**To inject a failure:**
```bash
kubectl delete pod $(kubectl get pods | grep cartservice | awk '{print $1}')
```
