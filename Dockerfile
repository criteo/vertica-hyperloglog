FROM gcc:8.5.0-buster
RUN echo deb http://archive.debian.org/debian buster-backports main contrib non-free | tee -a /etc/apt/sources.list
RUN apt-get update && apt-get -y install cmake/buster-backports
RUN mkdir -p /opt/vertica/sdk /sources /build
RUN useradd builder && chown builder: /opt/vertica /sources/ build
WORKDIR /build
USER builder
