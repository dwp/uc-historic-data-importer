#!/bin/bash

### dks https ###

dks_name=$(docker exec dks cat /etc/hosts | egrep -v '(localhost|ip6)' | tail -n1)

echo "dks https container is '${dks_name}'"

if [[ -n "${dks_name}" ]]; then

    temp_file=$(mktemp)
    (
        cat /etc/hosts | grep -v 'added by dks-uhdi.$'
        echo ${dks_name} local-dks \# added by dks-uhdi.
    ) > ${temp_file}

    sudo mv ${temp_file} /etc/hosts
    sudo chmod 644 /etc/hosts
    cat /etc/hosts
else
    (
        echo could not get host name from dks hosts file:
        docker exec dks cat /etc/hosts
    ) >&2
fi
echo "...hosts updated for dks https container '${dks_name}'"


### hbase ###

hbase_name=$(docker exec hbase cat /etc/hosts \
                 | egrep -v '(localhost|ip6)' | tail -n1)

echo "hbase container is '${hbase_name}'"

if [[ -n "$hbase_name" ]]; then

    temp_file=$(mktemp)
    (
        cat /etc/hosts | grep -v 'added by hbase-uhdi.$'
        echo ${hbase_name} local-hbase \# added by hbase-uhdi.
    ) > ${temp_file}

    sudo mv ${temp_file} /etc/hosts
    sudo chmod 644 /etc/hosts
    cat /etc/hosts
else
    (
        echo could not get host name from hbase hosts file:
        docker exec hbase cat /etc/hosts
    ) >&2
fi
echo "...hosts updated for hbase container '${dks_http_name}'"
