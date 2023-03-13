from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from parameterize_flow import etl_flow_wrapper

docker_block = DockerContainer.load("docker-container")
docker_deployment = Deployment.build_from_flow(flow=etl_flow_wrapper, name='docker-flow', infrastructure=docker_block)

if __name__ == '__main__':
  docker_deployment.apply()