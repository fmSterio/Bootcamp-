

## as we have alredy added our sours code to docker imange, why do we use here from etl_web_to_gcs import etl_parent_flow? I just dont get this part

from prefect.infrastructure.docker import DockerContainer
from prefect.deployments import Deployment
from etl_web_to_gcs import etl_parent_flow


docker_container_block = DockerContainer.load("zoomw2")

docker_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name= 'docker-flow',
    infrastructure = docker_container_block 
)

if __name__ == "__main__":
    docker_dep.apply()