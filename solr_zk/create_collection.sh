#!/usr/bin/env bash
set -e

SOLR_HOST="${SOLR_HOST:-localhost}"
SOLR_PORT="${SOLR_PORT:-8983}"
COLLECTION="${COLLECTION:-movies}"
CONFIGSET_NAME="${CONFIGSET_NAME:-movies_config}"
REPLICAS="${REPLICAS:-1}"
SHARDS="${SHARDS:-5}"

# Wait for Solr to be ready
echo "Waiting for Solr to be available at http://${SOLR_HOST}:${SOLR_PORT}/solr/ ..."
until curl -s "http://${SOLR_HOST}:${SOLR_PORT}/solr/admin/cores?action=STATUS" | grep -q "responseHeader"; do
  printf "."
  sleep 1
done
echo
echo "Solr is up."

# If collection already exists, skip create
exists=$(curl -s "http://${SOLR_HOST}:${SOLR_PORT}/solr/admin/collections?action=LIST" | jq -r '.collections[]?' | grep -w "${COLLECTION}" || true)
if [ -n "$exists" ]; then
  echo "Collection ${COLLECTION} already exists. Exiting init."
  exit 0
fi

# Create collection - using the configset directory (container has /opt/solr/server/solr/configsets/<CONFIGSET>)
echo "Creating collection ${COLLECTION} using configset ${CONFIGSET_NAME} ..."
curl "http://${SOLR_HOST}:${SOLR_PORT}/solr/admin/collections?action=CREATE&name=${COLLECTION}&numShards=${SHARDS}&replicationFactor=${REPLICAS}&collection.configName=${CONFIGSET_NAME}"
echo
echo "Collection ${COLLECTION} creation requested. Done."
