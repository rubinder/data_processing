FROM spark:3.5.7-scala2.12-java17-ubuntu

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER spark
