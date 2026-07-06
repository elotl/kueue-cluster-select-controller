kubectl patch workload $1 \
  -n default \
  --subresource=status \
  --type=merge \
  -p '{"status":{"nominatedClusterNames":["'"$2"'"]}}'
