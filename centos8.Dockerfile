FROM rockylinux:8.5.20220308
RUN dnf -y install cmake gcc-toolset-9 cmake gcc-toolset-9-gcc-c++
RUN mkdir -p /opt/vertica/sdk /sources /build
RUN useradd builder && chown builder: /opt/vertica /sources/ build
WORKDIR /build
USER builder
RUN echo 'source scl_source enable gcc-toolset-9' >> ~/.bashrc
RUN echo 'echo "# you may build using: cmake /sources; make;" 1>&2; echo;' >> ~/.bashrc
