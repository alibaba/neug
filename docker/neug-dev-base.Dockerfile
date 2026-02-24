FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo '$TZ' > /etc/timezone

ENV NEUG_HOME=/opt/neug
ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/usr/local/lib64
ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:$NEUG_HOME/lib:$NEUG_HOME/lib64
ENV PATH=$PATH:$NEUG_HOME/bin:/home/neug/.local/bin

RUN apt-get update && \
    apt-get install -y sudo vim python3-pip tzdata tmux && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/*

RUN useradd -m neug -u 1001 \
    && echo 'neug ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

RUN mkdir -p /opt/neug /opt/vineyard && chown -R neug:neug /opt/neug /opt/vineyard
USER neug
WORKDIR /home/neug
