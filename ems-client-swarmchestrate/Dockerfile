#
# Copyright (C) 2017-2026 Institute of Communication and Computer Systems (imu.iccs.gr)
#
# This Source Code Form is subject to the terms of the Mozilla Public License, v2.0, unless 
# Esper library is used, in which case it is subject to the terms of General Public License v2.0.
# If a copy of the MPL was not distributed with this file, you can obtain one at 
# https://www.mozilla.org/en-US/MPL/2.0/
#


# ----------------- EMS Builder image -----------------
FROM ems_core_builder_image AS ems-swarmchestrate-translator-builder

ARG BUILD_DIR=/build

# Accept optional metadata (CI can pass these, local builds ignore them)
ARG GIT_COMMIT=unknown
ARG GIT_BRANCH=unknown
ARG GIT_URL=unknown
ARG DOCKER_IMAGE=unknown
ARG BUILD_DESCR=''

ENV GIT_COMMIT=$GIT_COMMIT \
    GIT_BRANCH=$GIT_BRANCH \
    GIT_URL=$GIT_URL \
    DOCKER_IMAGE=$DOCKER_IMAGE \
    BUILD_DESCR="$BUILD_DESCR"

WORKDIR ${BUILD_DIR}/ems-nebulous-translator

# Download dependencies
#COPY ems-swarmchestrate/pom.xml   ${BUILD_DIR}/ems-nebulous-translator/pom.xml
#RUN --mount=type=cache,target=/root/.m2,id=maven-cache mvn -B dependency:go-offline
#RUN mvn -q -B -ntp dependency:go-offline

# Copy source and .git
COPY ./.git                     ${BUILD_DIR}/.git
COPY ems-client-swarmchestrate  ${BUILD_DIR}/ems-nebulous-translator

# Build plugin
#RUN --mount=type=cache,target=/root/.m2,id=maven-cache \
RUN \
    mvn -B -ntp -DskipTests \
    -Ddocker.image-nebulous=${DOCKER_IMAGE} \
    -Dbuild.description="${BUILD_DESCR}" \
    clean install -P '!build-docker-image'


# -----------------   EMS Server with Nebulous+Swarmchestrate Translator image   -----------------
FROM ems_core_image AS ems-server-with-swarmchestrate-translator

ARG BUILD_DIR=/build

ENV EXTRA_LOADER_PATHS=/plugins/* \
    SCAN_PACKAGES=eu.nebulous.ems,eu.swarmchestrate.ems \
    IP_SETTING=DEFAULT_IP \
    SELF_HEALING_ENABLED=false

COPY --from=ems-swarmchestrate-translator-builder ${BUILD_DIR}/ems-nebulous-translator/target/ems-nebulous-*-jar-with-dependencies.jar /plugins/
COPY --from=ems-swarmchestrate-translator-builder ${BUILD_DIR}/ems-nebulous-translator/target/banner.txt /tmp/

RUN cat /tmp/banner.txt >> ${BASEDIR}/BOOT-INF/classes/banner.txt


# -----------------   EMS Server with Nebulous+Swarmchestrate Translator image   -----------------
FROM ems_client_image AS ems-client-with-adaptive-filtering

ARG BUILD_DIR=/build

ENV SCAN_PACKAGES=eu.swarmchestrate.ems \
    IP_SETTING=DEFAULT_IP \
    SELF_HEALING_ENABLED=false \
    COLLECTOR_CLASSES=eu.swarmchestrate.ems.client.collector.netdata.K8sNetdataCollector,gr.iccs.imu.ems.baguette.client.collector.prometheus.PrometheusCollector2,gr.iccs.imu.ems.baguette.client.collector.generator.ClientGeneratorCollector

COPY --from=ems-swarmchestrate-translator-builder ${BUILD_DIR}/ems-nebulous-translator/target/ems-nebulous-*-jar-with-dependencies.jar ./jars/
