ARG ARCH=x86_64
ARG REGISTRY=neug-registry.cn-hongkong.cr.aliyuncs.com
ARG BASETAG=2026.03.13-2
FROM quay.io/pypa/manylinux_2_28_$ARCH:${BASETAG} AS builder

ENV DEBIAN_FRONTEND=noninteractive

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
