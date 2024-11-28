# Quick building/compilation guide

## Install Vertica SDK (or SDKs)

Obtain the SDK or SDKs you need from Vertica instances, from Vertica Docker images...

The active SDK should be accessible through `/opt/vertica/sdk` so that the path
`/opt/vertica/sdk/include/BuildInfo.h` exists and belongs to the wanted SDK.

This may be achieved through the following, assuming the contents of `/opt/vertica/sdk`
is in the tarball `/tmp/vertica-sdk-24.0.x.tgz`:

```bash
# 1: create the expected path for a Vertica SDK and make yourself its owner
sudo mkdir -p /opt/vertica/; sudo chown $(whoami): /opt/vertica
# 2: create the directory for SDKs in your home
mkdir -p $HOME/dev/vertica-sdk/24.0.x
# 3: extract the SDK to the directory you just created
tar -C $HOME/dev/vertica-sdk/24.0.x -xvf /tmp/vertica-sdk-24.0.x.tgz
# 4: make this SDK the active SDK
rm /opt/vertica/sdk; ln -s $HOME/dev/vertica-sdk/24.0.x /opt/vertica/sdk
# if you want to add other SDKs, repeat steps 2 and 3.
# if you want to enable another SDK, repeat step 4.
```

## Enable the Vertica SDK you want to compile against

See previous step.

## Install Docker

Refer to the documentation for your environment.

## Build and run the builder Docker image

Run `./start-build-env.sh` and follow the instructions.

This is an example run:

```bash
# ./start-build-env.sh
[+] Building 1.3s (12/12) FINISHED                                                                             docker:default
 => [internal] load build definition from centos8.Dockerfile                                                             0.0s
 => => transferring dockerfile: 435B                                                                                     0.0s
 => [internal] load metadata for docker.io/library/rockylinux:8.5.20220308                                               1.3s
 => [auth] library/rockylinux:pull token for registry-1.docker.io                                                        0.0s
 => [internal] load .dockerignore                                                                                        0.0s
 => => transferring context: 2B                                                                                          0.0s
 => [1/7] FROM docker.io/library/rockylinux:8.5.20220308@sha256:c7d13ea4d57355aaad6b6ebcdcca50f5be65fc821f54161430f5c25  0.0s
 => CACHED [2/7] RUN dnf -y install cmake gcc-toolset-9 cmake gcc-toolset-9-gcc-c++                                      0.0s
 => CACHED [3/7] RUN mkdir -p /opt/vertica/sdk /sources /build                                                           0.0s
 => CACHED [4/7] RUN useradd builder && chown builder: /opt/vertica /sources/ build                                      0.0s
 => CACHED [5/7] WORKDIR /build                                                                                          0.0s
 => CACHED [6/7] RUN echo 'source scl_source enable gcc-toolset-9' >> ~/.bashrc                                          0.0s
 => CACHED [7/7] RUN echo 'echo "# you may build using: cmake /sources; make;" 1>&2; echo;' >> ~/.bashrc                 0.0s
 => exporting to image                                                                                                   0.0s
 => => exporting layers                                                                                                  0.0s
 => => writing image sha256:d87191b04ef3897d1ec3962e7228acc36e8355e07634d60a937872c81da2b96c                             0.0s
 => => naming to docker.io/library/vertica-builder-hll-druid:local                                                       0.0s
vbuilder-hll-druid
Error response from daemon: No such container: vbuilder-hll-druid
94d81b98487f2aea8480969ad4f9c2ac5a3b36200c97aa626bf99e44c2d07a02
Run docker exec -ti vbuilder-hll-druid bash # now
Then, you may run the following to build
cmake /sources; make clean; make
```

## Perform any development you want

You may touch the source code in the SOURCES directory.

## Build and recover built assets

Within the container, run the following:

```bash
# you may want to cleanup the /build directory
rm -rf /build/*
# ensure you're in /build (you should be, by default)
cd /build
# prepare the environment with CMake
# you also want to run that if you modify the CMakeLists.txt  or other components
cmake /sources
# build the code
make
```

You will find the UDx as a .so file in the build directory of your local copy.

## Build for another SDK

- Perform the SDK change as indicated in this document.
- Then, **re-run start-build-env.sh** because the container needs to restart to take in
  account the new SDK.
- Just build again.
