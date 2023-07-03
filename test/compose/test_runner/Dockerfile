FROM erlang:25

RUN DEBIAN_FRONTEND=noninteractive \
    apt-get update && \
    apt-get install -y build-essential maven gosu unzip curl python3-pip python3-venv bind9-dnsutils less socat

RUN mkdir /home/work
WORKDIR /home/work



# RUN cd /tmp && \
#    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
#    unzip awscliv2.zip && \
#    ./aws/install

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

RUN touch /

COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
