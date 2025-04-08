# Coordinator of graphscope engines

ARG PLATFORM=x86_64
ARG ARCH=amd64
ARG REGISTRY=registry.cn-hongkong.aliyuncs.com
ARG VINEYARD_VERSION=latest
FROM $REGISTRY/graphscope/graphscope-dev:$VINEYARD_VERSION-$ARCH AS builder
ARG ENABLE_COORDINATOR="false"
ARG OPTIMIZE_FOR_HOST=OFF
ARG ENABLE_OPENTELMETRY=false
ARG PARALLEL=8

RUN sudo mkdir -p /opt/src && sudo chown -R graphscope:graphscope /opt/src/
USER graphscope
WORKDIR /home/graphscope

# change bash as default
SHELL ["/bin/bash", "-c"]

RUN if [ "${ENABLE_OPENTELMETRY}" = "true" ]; then \
    cd /tmp && git clone -b v1.14.2 --single-branch https://github.com/open-telemetry/opentelemetry-cpp && cd opentelemetry-cpp && \
    cmake . -DCMAKE_INSTALL_PREFIX=/opt/src -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_STANDARD=17 -DCMAKE_POSITION_INDEPENDENT_CODE=ON  -DBUILD_SHARED_LIBS=ON -DWITH_OTLP_HTTP=ON -DWITH_OTLP_GRPC=OFF -DWITH_ABSEIL=OFF -DWITH_PROMETHEUS=OFF -DBUILD_TESTING=OFF -DWITH_EXAMPLES=OFF && \
    make -j  && make install && rm -rf /tmp/opentelemetry-cpp; \
    fi

COPY --chown=graphscope:graphscope . /home/graphscope/GraphScope

# install src
RUN . ${HOME}/.cargo/env  && cd ${HOME}/GraphScope/src && \
    git submodule update --init && mkdir build && cd build && cmake .. -DCMAKE_INSTALL_PREFIX=/opt/src -DBUILD_DOC=OFF -DBUILD_TEST=OFF -DOPTIMIZE_FOR_HOST=${OPTIMIZE_FOR_HOST} -DUSE_STATIC_ARROW=ON && make -j ${PARALLEL} && make install && \
    cd ~/GraphScope/interactive_engine/ && mvn clean package -Pexperimental -DskipTests && \
    cd ~/GraphScope/interactive_engine/compiler && cp target/compiler-0.0.1-SNAPSHOT.jar /opt/src/lib/ && \
    cp target/libs/*.jar /opt/src/lib/ && \
    ls ~/GraphScope/interactive_engine/executor/ir && \
    cp ~/GraphScope/interactive_engine/executor/ir/target/release/libir_core.so /opt/src/lib/

# strip all .so in /opt/src/lib
RUN sudo find /opt/src/lib/ -name "*.so" -type f -exec strip {} \;
# strip all binary in /opt/src/bin
RUN sudo strip /opt/src/bin/bulk_loader /opt/src/bin/interactive_server /opt/src/bin/gen_code_from_plan

# build coordinator
RUN mkdir -p /opt/src/wheel
RUN if [ "${ENABLE_COORDINATOR}" = "true" ]; then \
    export PATH=${HOME}/.local/bin:${PATH} && \
    cd ${HOME}/GraphScope/src/interactive/sdk && \
    ./generate_sdk.sh -g python && cd python && \
    python3 -m pip install --upgrade pip && python3 -m pip install -r requirements.txt && \
    python3 setup.py build_proto && python3 setup.py bdist_wheel && \
    cp dist/*.whl /opt/src/wheel/ && \
    cd ${HOME}/GraphScope/python && \
    # Here is something hacky: There are many dependencies in graphscope client that are not used in src-interactive scenario, we modify the __init__.py to avoid
    # importing those dependencies. Otherwise it will cause error when running coordinator
    sed -i 's/from graphscope.* import.*//g' graphscope/__init__.py && \
    export WITHOUT_LEARNING_ENGINE=ON && python3 setup.py bdist_wheel && \
    cp dist/*.whl /opt/src/wheel/ && \
    cd ${HOME}/GraphScope/coordinator && \
    # remove all code like from graphscope.* import.* in utils.py, except for get_tempdir
    sed -i 's/^from graphscope\.framework.* import.*//g' gscoordinator/utils.py && \
    python3 setup.py bdist_wheel && \
    cp dist/*.whl /opt/src/wheel/; \
    fi


########################### RUNTIME IMAGE ###########################

from ubuntu:22.04 as runtime
ARG PLATFORM=x86_64
ARG ENABLE_COORDINATOR="false"

ENV DEBIAN_FRONTEND=noninteractive

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8
# g++ + jre 500MB
RUN apt-get update && apt-get -y install sudo locales g++ cmake openjdk-11-jre-headless tzdata && \
    locale-gen en_US.UTF-8 && apt-get clean -y && rm -rf /var/lib/apt/lists/* && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# python3
RUN if [ "${ENABLE_COORDINATOR}" = "true" ]; then \
    apt-get update && apt-get -y install python3 python3-pip && \
    apt-get clean -y && sudo rm -rf /var/lib/apt/lists/*; \
    fi

RUN mkdir /opt/vineyard/

# copy builder's /opt/src to final image
COPY --from=builder /opt/src/bin/interactive_server /opt/src/bin/bulk_loader \
    /opt/src/bin/gen_code_from_plan /opt/src/bin/

# copy wheel 
COPY --from=builder /opt/src/wheel/ /opt/src/wheel/

# lib 
COPY --from=builder /opt/src/lib/ /opt/src/lib/
# remove .a files
RUN find /opt/src/lib/ -name "*.a" -type f -delete

# include 
COPY --from=builder /opt/src/include/ /opt/graphscope/include/ /opt/vineyard/include/ /opt/src/include/
COPY --from=builder /opt/graphscope/lib/libgrape-lite.so /opt/src/lib/

# copy the builtin graph, modern_graph
RUN mkdir -p /opt/src/share/gs_interactive_default_graph/
COPY --from=builder /home/graphscope/GraphScope/src/interactive/examples/modern_graph/* /opt/src/share/gs_interactive_default_graph/
COPY --from=builder /home/graphscope/GraphScope/tests/engines/interactive_config_test.yaml /opt/src/share/interactive_config.yaml
COPY --from=builder /home/graphscope/docker/entrypoint.sh /opt/src/bin/entrypoint.sh
RUN sed -i 's/name: modern_graph/name: gs_interactive_default_graph/g' /opt/src/share/gs_interactive_default_graph/graph.yaml && \
    sed -i 's/default_graph: modern_graph/default_graph: gs_interactive_default_graph/g' /opt/src/share/interactive_config.yaml

# remove bin/run_app
RUN rm -rf /opt/src/bin/run_app

COPY --from=builder /usr/lib/$PLATFORM-linux-gnu/libsnappy*.so* /usr/lib/$PLATFORM-linux-gnu/
COPY --from=builder /usr/include/arrow /usr/include/arrow
COPY --from=builder /usr/include/yaml-cpp /usr/include/yaml-cpp
COPY --from=builder /usr/include/boost/filesystem* /usr/include/boost
COPY --from=builder /usr/include/boost/format* /usr/include/boost
COPY --from=builder /usr/include/google /usr/include/google
COPY --from=builder /usr/include/glog /usr/include/glog
COPY --from=builder /usr/include/gflags /usr/include/gflags
COPY --from=builder /usr/include/rapidjson /usr/include/rapidjson
COPY --from=builder /usr/lib/$PLATFORM-linux-gnu/openmpi/include/ /opt/src/include


COPY --from=builder /usr/lib/$PLATFORM-linux-gnu/libprotobuf* /usr/lib/$PLATFORM-linux-gnu/libfmt*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libre2*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libutf8proc*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libevent*.so*  \
    /usr/lib/$PLATFORM-linux-gnu/libltdl*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libltdl*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libopen-pal*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libunwind*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libhwloc*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libopen-rte*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libcrypto*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libboost_thread*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libboost_filesystem*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libboost_program_options*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libmpi*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libyaml-cpp*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libglog*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libgflags*.so* \
    /usr/lib/$PLATFORM-linux-gnu/libicudata.so* \
    /usr/lib/$PLATFORM-linux-gnu/

RUN sudo rm -rf /usr/lib/$PLATFORM-linux-gnu/libLLVM*.so* && sudo rm -rf /opt/src/lib/libseastar.a && \
    sudo rm -rf /usr/lib/$PLATFORM-linux-gnu/libcuda.so && \
    sudo rm -rf /usr/lib/$PLATFORM-linux-gnu/libcudart.so

RUN sudo ln -sf /opt/src/bin/* /usr/local/bin/ \
    && sudo ln -sfn /opt/src/include/* /usr/local/include/ \
    && sudo ln -sf -r /opt/src/lib/* /usr/local/lib \
    && sudo ln -sf /opt/src/lib64/*so* /usr/local/lib64 \
    && chmod +x /opt/src/bin/*

RUN if [ "${ENABLE_COORDINATOR}" = "true" ]; then \
    pip3 install --upgrade pip && \
    # Those python packages is not used in this image
    pip3 install /opt/src/wheel/*.whl && \ 
    pip3 uninstall pandas vineyard pyarrow botocore networkx numpy kubernetes \
    grpc etcd-distro grpcio-tools neo4j msgpack mypy-protobuf \
    Cython vineyard-io -y && \
    rm -rf ~/.cache/pip; \
    fi

ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/opt/src/lib/:/usr/lib/:/usr/local/lib/
# src solution
ENV SOLUTION=INTERACTIVE
ENV GRAPHSCOPE_RUNTIME=/tmp/gs/

# Add graphscope user with user id 1001
RUN useradd -m graphscope -u 1001 && \
    echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers && \
    chown -R graphscope:graphscope /opt/src

# set home to graphscope user
ENV HOME=/home/graphscope
USER graphscope
WORKDIR /home/graphscope

ENTRYPOINT ["/opt/src/bin/entrypoint.sh"]
