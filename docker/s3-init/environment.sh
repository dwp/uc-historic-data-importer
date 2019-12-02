#! /bin/bash

stderr() {
    echo $@ >&2
}

aws_s3() {
    local subcommand=${1:?Usage: $FUNCNAME sub-command [...args]}
    shift
    aws_cmd s3 $subcommand $@
}

aws_s3api() {
    local subcommand=${1:?Usage: $FUNCNAME sub-command [...args]}
    shift
    aws_cmd s3api $subcommand $@
}

aws_cmd() {
    local command=${1:?Usage: $FUNCNAME command sub-command [...args]}
    local subcommand=${2:?Usage: $FUNCNAME command sub-command [...args]}
    shift 2
    local exec="aws --endpoint-url=${S3_SERVICE_ENDPOINT} \
                    --region ${AWS_REGION} $command $subcommand $@"
    stderr $FUNCNAME: exec: \'$exec\'.
    $exec
    stderr $FUNCNAME: \'$command $subcommand\' exited with return code \'$?\'.
    return $?
}

create_sample_data() {
    ./sample_data.py -n5 -s10 -cedimk http://dks-insecure:8080/datakey
}

create_paginated_data() {
    for i in $(seq 1 510); do
        local data_file=database$i.collection$i.$i.json.gz.enc
        local metadata_file=database$i.collection$i.$i.json.gz.encryption.json
        dd if=/dev/zero of=$data_file bs=1 count=1
        dd if=/dev/zero of=$metadata_file bs=1 count=1
        stderr Copying pair \'$i\'.
        aws_s3 cp $data_file s3://${S3_BUCKET}/${S3_PAGING_PREFIX}/${data_file}
        aws_s3 cp $metadata_file s3://${S3_BUCKET}/${S3_PAGING_PREFIX}/${metadata_file}
    done
}

create_voluminous_data() {
    for file in database-1.collection-1.*; do
        aws --endpoint-url=$(aws_s3_endpoint) s3 cp \
            $file s3://$(aws_s3_prefix)/$file
    done

}

aws_clear_down() {
    aws --endpoint-url=$(aws_s3_endpoint) s3 ls $(aws_s3_prefix)/  | \
        awk '{print $4}' | while read; do
        aws --endpoint-url=$(aws_s3_endpoint) s3 rm s3://$(aws_s3_prefix)/$REPLY
    done
}

aws_s3_endpoint() {
    echo http://s3:4572
}


aws_s3_prefix() {
    echo uc-historic-data/test/prefix
}
