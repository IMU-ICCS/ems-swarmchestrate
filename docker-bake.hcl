variable "REGISTRY" {
  default = "ghcr.io/imu-iccs"
}

# Exact digests resolved at build time by the preflight job - see workflow.
# Falling back to a floating tag only matters for local/manual builds.
variable "CORE_SERVER_REF" {
  default = "ghcr.io/imu-iccs/ems-server-nebulous:1.1.0-snapshot"
}

variable "CORE_BUILDER_REF" {
  default = "ghcr.io/imu-iccs/ems-server-nebulous-builder:1.1.0-snapshot"
}

variable "BANNER_IMAGE" {
  default = "na"
}

variable "BANNER_DESCR" {
  default = ""
}

variable "COMMIT_SHA" {
  default = "unknown-sha"
}

# variable "COMMIT_SAFE_TIME" {
#   default = "unknown-time"
# }

# Single platform per bake invocation - set per matrix leg in CI
variable "PLATFORM" {
  default = "linux/amd64"
}

variable "ARCH_TAG" {
  default = "amd64"
}

# "gha" (default, used in CI) or "local" (for local `docker buildx bake` runs)
variable "CACHE_TYPE" {
  default = "gha"
}

# Only used when CACHE_TYPE = "local"
variable "CACHE_DIR" {
  default = "./ems-cache"
}

# Only used when CACHE_TYPE = "registry"
variable "CACHE_REGISTRY" {
  default = "localhost:5000"
}


group "default" {
#   targets = ["builder", "plugin"]
  targets = ["plugin"]
}

target "common" {
  context    = "."
  dockerfile = "Dockerfile"

  platforms = [PLATFORM]

  # docker-image:// contexts resolve the correct per-platform manifest
  # automatically, even from a manifest-list ref - no arch suffix needed.
  contexts = {
    ems_core_builder_image = "docker-image://${CORE_BUILDER_REF}"
    ems_core_image         = "docker-image://${CORE_SERVER_REF}"
  }

  args = {
    DOCKER_IMAGE = "${BANNER_IMAGE}"
    BUILD_DESCR  = "${BANNER_DESCR}"
  }

  cache-from = ( CACHE_TYPE == "local"
                ? ["type=local,src=${CACHE_DIR}"]
                : CACHE_TYPE == "registry"
                        ? ["type=registry,ref=${CACHE_REGISTRY}/ems-server-swarmchestrate-cache:${ARCH_TAG}"]
                        : ["type=gha,scope=ems-swarmchestrate-${ARCH_TAG}"]
                )
  cache-to   = ( CACHE_TYPE == "local"
                ? ["type=local,dest=${CACHE_DIR},mode=max"]
                :  CACHE_TYPE == "registry"
                        ? ["type=registry,ref=${CACHE_REGISTRY}/ems-server-swarmchestrate-cache:${ARCH_TAG},mode=max"]
                        : ["type=gha,scope=ems-swarmchestrate-${ARCH_TAG},mode=max"]
                )
}

target "builder" {
  inherits = ["common"]
  target   = "ems-swarmchestrate-translator-builder"
  tags = [
    "${REGISTRY}/ems-server-swarmchestrate-builder:${COMMIT_SHA}-${ARCH_TAG}",
  ]
}

target "plugin" {
  inherits = ["common"]
  target     = "ems-server-with-swarmchestrate-translator"

  tags = [
    "${REGISTRY}/ems-server-swarmchestrate:${COMMIT_SHA}-${ARCH_TAG}",
  ]
}
