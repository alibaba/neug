ARG ARCH=x86_64
ARG REGISTRY=registry.cn-hongkong.aliyuncs.com
FROM quay.io/pypa/manylinux2014_$ARCH:2025.03.23-1 AS builder

ENV DEBIAN_FRONTEND=noninteractive

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo '$TZ' > /etc/timezone

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

RUN mkdir /opt/graphscope /opt/vineyard && chown -R graphscope:graphscope /opt/graphscope /opt/vineyard
# For output logs
RUN mkdir -p /var/log/graphscope && chown -R graphscope:graphscope /var/log/graphscope

COPY . /root/nexg
RUN cd /root/nexg && bash scripts/install_deps.sh && \
    cd /root && rm -rf /root/nexg && \
    source ~/.graphscope_env

    # change bash as default
SHELL ["/bin/bash", "-c"]

RUN echo ". ~/.graphscope_env" >> /root/.bashrc
