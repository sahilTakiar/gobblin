#!/usr/bin/env bash

TARGET_DIR="$(dirname $0)/target"
ES_VERSION=2.4.5
ES_DIR=${TARGET_DIR}/elasticsearch-${ES_VERSION}
mkdir -p ${TARGET_DIR}

ES_TAR=${TARGET_DIR}/elasticsearch-${ES_VERSION}.tar.gz
ES_URL=https://download.elastic.co/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/2.4.5/elasticsearch-2.4.5.tar.gz
echo ${ES_URL}
echo ${ES_TAR}
curl -o ${ES_TAR} ${ES_URL}
tar -xzf ${ES_TAR} -C ${TARGET_DIR}

cd ${ES_DIR}
bin/elasticsearch -d -p pid

sleep 10s
curl "http://localhost:9200/_cluster/health?wait_for_status=yellow&timeout=30s"
