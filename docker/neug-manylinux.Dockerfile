ARG ARCH=x86_64
ARG REGISTRY=neug-registry.cn-hongkong.cr.aliyuncs.com
ARG MANYLINUXVERSION=2014
FROM quay.io/pypa/manylinux${MANYLINUXVERSION}_$ARCH:latest AS builder
# Got issue with manylinux2014: https://github.com/grpc/grpc/issues/30218, upgrade to manylinux_2_28

ENV DEBIAN_FRONTEND=noninteractive


# RUN sed -i "s/mirror.centos.org/vault.centos.org/g" /etc/yum.repos.d/*.repo && \
#     sed -i "s/^#.*baseurl=http/baseurl=http/g" /etc/yum.repos.d/*.repo && \
#     sed -i "s/^mirrorlist=http/#mirrorlist=http/g" /etc/yum.repos.d/*.repo

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo '$TZ' > /etc/timezone

RUN useradd -m neug -u 1001 \
    && echo 'neug ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

RUN mkdir /opt/neug /opt/vineyard && chown -R neug:neug /opt/neug /opt/vineyard
# For output logs
RUN mkdir -p /var/log/neug && chown -R neug:neug /var/log/neug

COPY scripts/install_deps.sh /root/install_deps.sh
RUN cd /root/ && bash install_deps.sh

# change bash as default
SHELL ["/bin/bash", "-c"]

RUN echo ". ~/.neug_env" >> /root/.bashrc

# Setup environment
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk
