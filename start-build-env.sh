#!/bin/bash
_b_env=${1:-centos8.Dockerfile}
errout() {
  _r=$1
  shift 1;
  echo $@;
  exit $_r;
}
BUILDER_IMAGE="vertica-builder-hyperloglog:local"
BUILDER_INSTANCE_NAME="vbuilder-hyperloglog"
docker build -f ${_b_env} -t ${BUILDER_IMAGE} .
test -f /opt/vertica/sdk/include/BuildInfo.h || errout 30 "Vertica SDK is missing. Put it in /opt/vertica/sdk so that /opt/vertica/sdk/include/BuildInfo.h exists"

rm -rf build
mkdir -p build
docker kill ${BUILDER_INSTANCE_NAME} || true
sleep 1
docker rm ${BUILDER_INSTANCE_NAME} || true
sleep 1
docker run --rm --name ${BUILDER_INSTANCE_NAME} -v=$PWD/SOURCES:/sources -v=$PWD/build:/build -v=/opt/vertica/sdk:/opt/vertica/sdk  -d $BUILDER_IMAGE sleep 3600
echo "Run docker exec -ti ${BUILDER_INSTANCE_NAME} bash # now"
echo "Then, you may run the following to build"
echo "cmake /sources; make clean; make"
