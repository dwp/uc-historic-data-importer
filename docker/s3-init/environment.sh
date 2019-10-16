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
