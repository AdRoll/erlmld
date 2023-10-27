#!/bin/bash

set -x

######################################
#                                    #
#   Trust generated CA Certificate   #
#                                    #
######################################

sudo openssl rsa -noout -modulus -in /certs/server-key.pem
sudo openssl x509 -noout -modulus -in /certs/server-crt.pem
sudo openssl rsa -passin pass:foobar -noout -modulus -in /certs/ca-key.pem
sudo openssl x509 -noout -modulus -in /certs/ca-crt.pem

sudo cp /certs/ca-crt.pem /usr/local/share/ca-certificates/acme.crt
sudo update-ca-certificates

for cafile in $(find /usr/local/lib/aws-cli -name 'cacert.pem'); do
    sudo cp --no-clobber $cafile "${cafile}.orig"
    sudo cp "${cafile}.orig" $cafile
    sudo sh -c "openssl x509 -in /certs/ca-crt.pem -text >> $cafile"
done

openssl s_client -connect dynamodb.us-east-1.amazonaws.com:443
export AWS_ACCESS_KEY_ID=phony
export AWS_SECRET_ACCESS_KEY=fake
export AWS_DEFAULT_REGION=us-east-1

dig kinesis.us-east-1.amazonaws.com

git config --global --add safe.directory /home/work

aws kinesis list-streams || exit $?
aws dynamodb list-tables || exit $?

SHELL="/bin/bash" eval "$@"
