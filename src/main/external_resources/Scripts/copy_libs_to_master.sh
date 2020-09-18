#!/usr/bin/env bash

export CONF_BASE_DIR="/Users/shek21/ResearchApps/test-nested-why-not-spark"
export RESOURCE_DIR=${CONF_BASE_DIR}/src/main/external_resources
export PROV_LIB=${CONF_BASE_DIR}/target/nested-why-not-0.1-SNAPSHOT.jar

scp -i /Users/shek21/.ssh/id_rsa ${PROV_LIB} hadoop@bigmaster:/data/diesterf/libs
scp -i /Users/shek21/.ssh/id_rsa ${RESOURCE_DIR}/Scripts/submit.sh hadoop@bigmaster:/data/diesterf/libs
scp -i /Users/shek21/.ssh/id_rsa ${RESOURCE_DIR}/Scripts/run-tests* hadoop@bigmaster:/data/diesterf/libs
scp -i /Users/shek21/.ssh/id_rsa ${RESOURCE_DIR}/Scripts/get_results.sh hadoop@bigmaster:/data/diesterf/libs

echo "done"