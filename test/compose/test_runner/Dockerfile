FROM --platform=linux/amd64 erlang:25

RUN DEBIAN_FRONTEND=noninteractive \
    apt-get update && \
    apt-get install -y build-essential maven gosu unzip curl python3-pip python3-venv bind9-dnsutils less socat sudo


RUN mkdir /build && cd /build && \
    pip3 install 'flit_core>=3.7.1,<3.8.1' && \
    curl https://awscli.amazonaws.com/awscli.tar.gz -o awscli.tar.gz && \
    tar xzf awscli.tar.gz && \
    cd $(ls | grep awscli-) && \
    ./configure --with-download-deps && \
    make && make install && aws --version

RUN cd /bin && \
    wget https://s3.amazonaws.com/rebar3/rebar3 && \
    chmod +x rebar3

RUN useradd -rm -d /home/work -s /bin/bash -g root -G sudo -u 1001 adroll
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> \
/etc/sudoers

USER adroll
RUN mkdir /home/work/code
WORKDIR /home/work/code

COPY --chown=adroll ./priv/download.sh /home/work/code/priv/
RUN priv/download.sh
COPY --chown=adroll . /home/work/code/
ENTRYPOINT ["/home/work/code/test/compose/test_runner/entrypoint.sh"]
