# syntax=docker/dockerfile:1
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN rm -f /etc/apt/apt.conf.d/docker-clean; echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
  --mount=type=cache,target=/var/lib/apt,sharing=locked \
  apt update && \
  apt-get install -y -V \
    clang \
    cmake \
    ninja-build \
    libssl-dev \
    ca-certificates \
    build-essential \
    libboost-all-dev \
    git-all \
    lsb-release wget

RUN cd /home && \
    git clone https://github.com/apache/arrow.git && \
    mkdir -p arrow/cpp/build && \
    cd arrow/cpp/build && \
    cmake .. --preset ninja-release -DARROW_FLIGHT="ON" && \
    cmake --build . && \
    cmake --install .

RUN ldconfig

ENTRYPOINT /bin/bash

# to build
#  DOCKER_BUILDKIT=1 docker build . -t arrow_stuff 
# to run issue the following command
#  docker run -it --mount src=$(pwd)/src,target=/mnt/src,type=bind arrow_stuff
