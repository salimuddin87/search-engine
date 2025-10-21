#!/usr/bin/env bash
set -e

# Delete a schema if exists
curl -X DELETE http://localhost:8983/api/configsets/movies_config?omitHeader=true

# zip from the directory that contains movies_config/
zip -r movies_config.zip ./configsets/movies_config

# Upload a schema to solr
curl "http://localhost:8983/solr/admin/configs?action=UPLOAD&name=movies_config" \
  --data-binary "@movies_config.zip" \
  -H "Content-type:application/octet-stream"
