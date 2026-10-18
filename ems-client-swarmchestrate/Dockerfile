#
# Copyright (C) 2017-2026 Institute of Communication and Computer Systems (imu.iccs.gr)
#
# This Source Code Form is subject to the terms of the Mozilla Public License, v2.0, unless 
# Esper library is used, in which case it is subject to the terms of General Public License v2.0.
# If a copy of the MPL was not distributed with this file, you can obtain one at 
# https://www.mozilla.org/en-US/MPL/2.0/
#


# ----------------- EMS client Builder image -----------------
FROM ems_core_builder_image AS ems-client-swarmchestrate-builder

ARG BUILD_DIR=/build

# Accept optional metadata (CI can pass these, local builds ignore them)
ARG GIT_COMMIT=unknown
ARG GIT_BRANCH=unknown
ARG GIT_URL=unknown
ARG DOCKER_IMAGE=unknown
ARG BUILD_DESCR=''

ENV GIT_COMMIT=$GIT_COMMIT \
    GIT_BRANCH=$GIT_BRANCH \
    GIT_URL=$GIT_URL

WORKDIR ${BUILD_DIR}/ems-client-swarmchestrate

# Download dependencies
COPY ./pom.xml      ${BUILD_DIR}/ems-client-swarmchestrate/pom.xml
RUN mvn -q -B -ntp dependency:go-offline

# Copy source
COPY .  ${BUILD_DIR}/ems-client-swarmchestrate/

# Build plugin
RUN \
    mvn -B -ntp -DskipTests \
    clean install \
    dependency:copy-dependencies


# -----------------   EMS client with metrics filter image   -----------------
FROM ems_core_image AS ems-client-swarmchestrate

ARG BUILD_DIR=/build

ENV SCAN_PACKAGES=eu.swarmchestrate.ems \
    IP_SETTING=DEFAULT_IP         \
    LOGGING_LEVEL_EU_SWARMCHESTRATE_EMS_CLIENT_FILTER=INFO

RUN --mount=type=bind,from=ems-client-swarmchestrate-builder,source=${BUILD_DIR}/ems-client-swarmchestrate/target,target=/tmp/target \
    cp -f  /tmp/target/ems-client-swarmchestrate-*.jar  /opt/baguette-client/jars/ && \
    cp -rn /tmp/target/dependency/*                     /opt/baguette-client/jars/
