FROM neug-registry.cn-hongkong.cr.aliyuncs.com/neug/neug-dev:v0.1.1 as builder

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
RUN bash -c "source /home/neug/.neug_env && make python-dev && make python-wheel"
RUN bash -c "sudo apt install patchelf -y"
RUN bash -c "pip install auditwheel && auditwheel repair -w tools/python_bind/dist/wheelhouse tools/python_bind/dist/*.whl"

FROM ubuntu:22.04

WORKDIR /root
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /home/neug/neug/tools/python_bind/dist/wheelhouse/*.whl .
RUN python3 -m pip  install ./*.whl
RUN rm *.whl