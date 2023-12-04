ARG debian_buster_image_tag=8-jre-slim
FROM openjdk:${debian_buster_image_tag}

# -- Layer: OS + Python 3.9

ARG shared_workspace=/opt/workspace

RUN mkdir -p ${shared_workspace} && apt-get update
RUN apt install -y curl gcc
RUN apt install -y build-essential zlib1g-dev libncurses5-dev
RUN apt install -y libsqlite3-dev
RUN apt install -y libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev wget libjpeg-dev
RUN curl -O https://www.python.org/ftp/python/3.9.6/Python-3.9.6.tar.xz  && \
    tar -xf Python-3.9.6.tar.xz && cd Python-3.9.6 && ./configure && make -j 8 &&\
    make install
RUN apt-get update && apt-get install -y procps && apt-get install -y vim && apt-get install -y net-tools && \
    rm -rf /var/lib/apt/lists/*

ENV SHARED_WORKSPACE=${shared_workspace}

# -- Runtime

VOLUME ${shared_workspace}
CMD ["bash"]
