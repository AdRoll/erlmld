#!/bin/bash
set -x

KEY_SIZE=2048

rm -rf ca
rm /certs/*
mkdir -p ca/ca.db.certs
touch ca/ca.db.index
echo "1234" > ca/ca.db.serial
openssl genrsa -des3 -passout pass:foobar -out ca/ca.key $KEY_SIZE || exit 1

openssl req -new -x509 -passin pass:foobar \
        -subj "/C=US/ST=CA/O=ACME/CN=acme.fake/emailAddress=wcoyote@acme.fake" \
        -days 10000 -key ca/ca.key -out ca/ca.crt || exit 2

openssl genrsa -out server-key.pem $KEY_SIZE || exit 3

REGION_DOMAIN="us-east-1.amazonaws.com"
mk_san() {
    cat <<EOF | while read sub; do echo -n "${SEP}DNS:${sub}.${REGION_DOMAIN}"; SEP=","; done
kinesis
dynamodb
000000000000.data-kinesis
EOF
}
SAN=$(mk_san)
echo $SAN
openssl req -new \
        -subj "/C=US/ST=CA/O=Nile Web Services/CN=*.$REGION_DOMAIN" \
        -addext "subjectAltName=$SAN" \
        -key server-key.pem -out server-csr.pem || exit 4

openssl ca -config ca.conf -passin pass:foobar -batch -out server-crt.pem -infiles server-csr.pem || exit 5

cp server*.pem /certs
cp ca/ca.crt /certs/ca-crt.pem
cp ca/ca.key /certs/ca-key.pem

chown nobody.nogroup /certs/*
chmod a+r /certs/server-crt.pem /certs/ca-key.pem

sleep infinity
