FROM buildpack-deps:bullseye

RUN DEBIAN_FRONTEND=noninteractive \
    apt-get update && \
    apt-get install -y openssl

RUN mkdir /ca
RUN mkdir /certs
WORKDIR /ca

COPY cacert.sh /ca
COPY check_cert.sh /ca
COPY ca.conf /ca

HEALTHCHECK --interval=5s CMD /ca/check_cert.sh

CMD ./cacert.sh