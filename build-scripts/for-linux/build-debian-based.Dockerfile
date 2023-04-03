# Any debian-based image is suitable
ARG IMAGE=ubuntu:18.04
FROM ${IMAGE}

ARG PROJECT_NAME
ARG FOR_OS=linux

# The prroject directory must be mounted to /mnt/project

# Set up the runner user
RUN \
  apt update \
  && apt install -y sudo apt-utils \
  && useradd -mN runner \
  && echo "runner ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/90-runner

USER runner
WORKDIR /home/runner

# Install dependencies
RUN /mnt/project/build-scripts/for-${FOR_OS}/prepare-debian-based.bash

LABEL org.opencontainers.image.description="An image for building ${PROJECT_NAME} for ${FOR_OS} from sources"
