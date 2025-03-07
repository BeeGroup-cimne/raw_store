## Kubernetes configuration

This YAML files (`services.yaml`) defines the specifications for Kubernetes resources, 
which is used to schedule recurring tasks within a Kubernetes cluster. 
By configuring these resources, the Kubernetes cluster can handle scheduled data 
ingestion and processing tasks efficiently, ensuring the system remains up-to-date without
manual intervention.

In this configuration, the yaml files schedules Deployments.

---
## Containers Defined

Each yaml file defines different resources

### File `services.yaml`
* Two containers defined

  * name: raw-store
    * image: raw-store
    * command: `python3 __main__.py`
    * replicas: 10
    
  * name: raw-store-prod
    * image: raw-store
    * command: `python3 __main__.py`
    * replicas: 10

