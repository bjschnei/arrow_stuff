# syntax=docker/dockerfile:1
FROM ubuntu
RUN rm -f /etc/apt/apt.conf.d/docker-clean; echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
  --mount=type=cache,target=/var/lib/apt,sharing=locked \
  apt update && \
  apt-get install -y \
    clang \
    cmake \
    ca-certificates \
    lsb-release wget

RUN wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get install -y ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
  apt update && \
  apt-get install -y -V \
     libarrow-dev `# For C++` \
     libarrow-glib-dev `# For GLib (C)` \
     libarrow-dataset-dev `# For Apache Arrow Dataset C++` \
     libarrow-dataset-glib-dev `# For Apache Arrow Dataset GLib (C)` \
     libarrow-acero-dev `# For Apache Arrow Acero` \
     libarrow-flight-dev `# For Apache Arrow Flight C++` \
     libarrow-flight-glib-dev `# For Apache Arrow Flight GLib (C)` \
     libarrow-flight-sql-dev `# For Apache Arrow Flight SQL C++` \
     libarrow-flight-sql-glib-dev `# For Apache Arrow Flight SQL GLib (C)` \
     libgandiva-dev `# For Gandiva C++` \
     libgandiva-glib-dev `# For Gandiva GLib (C)` \
     libparquet-dev `# For Apache Parquet C++` \
     libparquet-glib-dev `# For Apache Parquet GLib (C)`

ENTRYPOINT /bin/bash

# to build
#  DOCKER_BUILDKIT=1 docker build . -t arrow_stuff 
# to run issue the following command
#  docker run -it --mount src=$(pwd)/src,target=/mnt/src,type=bind arrow_stuff
