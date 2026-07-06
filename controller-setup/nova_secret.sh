kubectl create secret generic target-api-kubeconfig \
  --from-file=kubeconfig=${HOME}/.nova/nova/nova-kubeconfig \
  -n kueue-cluster-select-system
