kubectl create secret docker-registry regcred \
  --docker-server=docker.io \
  --docker-username=${DOCKER_USERNAME} \
  --docker-password=${DOCKER_PASSWORD} \
  -n kueue-cluster-select-system
