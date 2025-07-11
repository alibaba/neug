ARG ARCH=x86_64
ARG REGISTRY=registry.cn-hongkong.aliyuncs.com
FROM graphscope/neug-manylinux:$ARCH as builder

ENV DEBIAN_FRONTEND=noninteractive

COPY scripts/install_deps.sh /root/install_deps.sh
RUN cd /root/ && source ~/.graphscope_env && bash install_deps.sh --brpc && \
    rm /root/install_deps.sh

