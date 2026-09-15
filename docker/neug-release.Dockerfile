FROM neug-registry.cn-hongkong.cr.aliyuncs.com/neug/neug-dev:v0.2.0 as builder

ARG NEUG_PACKAGE_BUILD=ON
ARG NEUG_NATIVE_ARCH=OFF

USER neug

RUN mkdir -p /home/neug/neug
COPY . /home/neug/neug
RUN bash -c "sudo chown -R neug:neug neug"

WORKDIR /home/neug/neug
ENV BUILD_EXECUTABLES=ON
ENV BUILD_HTTP_SERVER=ON
ENV WITH_MIMALLOC=ON
ENV ENABLE_BACKTRACES=OFF
ENV BUILD_TYPE=RELEASE
ENV BUILD_TEST=OFF
RUN bash -c "source /home/neug/.neug_env && make python-dev EXTRA_CMAKE_FLAGS=\"-DNEUG_PACKAGE_BUILD=${NEUG_PACKAGE_BUILD} -DNEUG_NATIVE_ARCH=${NEUG_NATIVE_ARCH}\" && make python-wheel"
# The builder image (Ubuntu 22.04 / glibc 2.35) produces a linux_x86_64 wheel with
# symbols too recent for manylinux_2_5/manylinux2014. The release image is also
# Ubuntu 22.04, so the platform-specific wheel works as-is; skip auditwheel repair.
RUN bash -c "mkdir -p tools/python_bind/dist/wheelhouse && cp tools/python_bind/dist/*.whl tools/python_bind/dist/wheelhouse/"

FROM ubuntu:22.04

WORKDIR /root
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /home/neug/neug/tools/python_bind/dist/wheelhouse/*.whl .
RUN python3 -m pip  install ./*.whl
RUN rm *.whl
