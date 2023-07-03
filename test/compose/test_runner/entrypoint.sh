#!/bin/bash

set -x

######################################
#                                    #
#   Trust generated CA Certificate   #
#                                    #
######################################

openssl rsa -noout -modulus -in /certs/server-key.pem
openssl x509 -noout -modulus -in /certs/server-crt.pem
openssl rsa -passin pass:foobar -noout -modulus -in /certs/ca-key.pem
openssl x509 -noout -modulus -in /certs/ca-crt.pem

cp /certs/ca-crt.pem /usr/local/share/ca-certificates/acme.crt
update-ca-certificates

for cafile in $(find /usr/local/lib/aws-cli -name 'cacert.pem'); do
    cp --no-clobber $cafile "${cafile}.orig"
    cp "${cafile}.orig" $cafile
    openssl x509 -in /certs/ca-crt.pem -text >> $cafile
done

openssl s_client -connect dynamodb.us-east-1.amazonaws.com:443
export AWS_ACCESS_KEY_ID=phony
export AWS_SECRET_ACCESS_KEY=fake
export AWS_DEFAULT_REGION=us-east-1

dig kinesis.us-east-1.amazonaws.com

git config --global --add safe.directory /home/work

aws kinesis list-streams || exit $?
aws dynamodb list-tables || exit $?

###########################################
#                                         #
#  Create and use a user that matches     #
#  uid/gid with the owner of the work dir #
#                                         #
###########################################

if [ "$(id -u)" = "0" ]; then
    echo $(ls -nd . | cut -d' ' -f3,4) |
        while read uid gid
        do
            groupadd -g $gid work_user
            useradd -u $uid -g $gid work_user -d /home/work
        done
    # insert gosu command into $@
    set -- gosu work_user "$@"
fi

SHELL="/bin/bash" eval "$@"
