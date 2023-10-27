#!/bin/bash
SRV_KEY_MOD=$(openssl rsa -noout -modulus -in /certs/server-key.pem)
SRV_CRT_MOD=$(openssl x509 -noout -modulus -in /certs/server-crt.pem)
CA_KEY_MOD=$(openssl rsa -passin pass:foobar -noout -modulus -in /certs/ca-key.pem)
CA_CRT_MOD=$(openssl x509 -noout -modulus -in /certs/ca-crt.pem)

if [ "$SRV_KEY_MOD" != "$SRV_CRT_MOD" ]; then
    echo "Server cert/key mismatch" 2>&1
    exit 1
fi
if [ "$CA_KEY_MOD" != "$CA_CRT_MOD" ]; then
    echo "CA cert/key mismatch" 2>&1
    exit 1
fi

openssl verify -CAfile /certs/ca-crt.pem /certs/server-crt.pem || exit 3

