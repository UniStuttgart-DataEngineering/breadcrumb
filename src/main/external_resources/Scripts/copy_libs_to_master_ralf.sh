#!/usr/bin/env bash

export CONF_BASE_DIR="/Users/Ralf/Documents/Nested-Why-Not-Implementation/nested-why-not-spark"
export RESOURCE_DIR=${CONF_BASE_DIR}/src/main/external_resources
export PROV_LIB=${CONF_BASE_DIR}/target/nested-why-not-0.1-SNAPSHOT.jar

scp -i /Users/ralf/.ssh/id_rsa ${PROV_LIB} hadoop@bigmaster:/data/diesterf/libs
scp -i /Users/ralf/.ssh/id_rsa ${RESOURCE_DIR}/Scripts/submit.sh hadoop@bigmaster:/data/diesterf/libs
scp -i /Users/ralf/.ssh/id_rsa ${RESOURCE_DIR}/Scripts/run-tests.sh hadoop@bigmaster:/data/diesterf/libs
scp -i /Users/ralf/.ssh/id_rsa ${RESOURCE_DIR}/Scripts/get-results.sh hadoop@bigmaster:/data/diesterf/libs

echo "done"