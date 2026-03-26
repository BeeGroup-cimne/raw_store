## Kubernetes configuration

This YAML files (`services.yaml`) defines the specifications for Kubernetes resources, 
which is used to schedule recurring tasks within a Kubernetes cluster. 
By configuring these resources, the Kubernetes cluster can handle scheduled data 
ingestion and processing tasks efficiently, ensuring the system remains up-to-date without
manual intervention.

In this configuration, the yaml files schedules Deployments.

---
## Containers Defined


### File `services.yaml`

* name: raw-store-prod
  * image: raw-store
  * command: `python3 __main__.py`
  * replicas: 10
  * type: Deployment

