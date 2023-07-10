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

if [ "$(id -u)" -eq "0" ]; then
    uid=$(ls -nd ./code | cut -d' ' -f3)
    gid=$(ls -nd ./code | cut -d' ' -f4)
    if [ "$uid" -eq "0" -o "$gid" -eq "0" ]; then
        groupadd work_user
        useradd -g work_user work_user -d /home/work
        cp -r /home/work/code /home/work/copy
        chown -R work_user:work_user /home/work/copy
        cd /home/work/copy
        set +x
        cat <<EOF

*********************************************************************************
**                                                                             **
**                            N  O  T  I  C  E                                 **
**                                                                             **
** --------------------------------------------------------------------------- **
**                                                                             **
** You are running docker as root, and your working dir is owned by root.      **
** This is probaby due to using docker-desktop (maybe on MacOs or Windows).    **
**                                                                             **
** Output files will not be present in your working dir. To retrieve, run:     **
**                                                                             **
** docker compose -f test/compose.yml cp tests:/home/work/copy/_build ./_out   **
**                                                                             **
*********************************************************************************

EOF
        set -x
    else
        groupadd -g $gid work_user
        useradd -u $uid -g $gid work_user -d /home/work
        cd /home/work/code
    fi
    chown work_user:work_user /home/work
    # insert gosu command into $@
    ls -ld /home/work
    set -- gosu work_user "$@"
fi

SHELL="/bin/bash" eval "$@"
