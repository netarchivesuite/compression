#!/usr/bin/env bash

#
# Compress an arc/warc file to a target checksum, deleting the original
#

trim()
{
    local var="$*"
    var="${var#"${var%%[![:space:]]*}"}"   # remove leading whitespace characters
    var="${var%"${var##*[![:space:]]}"}"   # remove trailing whitespace characters
    echo -n "$var"
}

usage()
{
    echo $1
    echo Usage: compress.sh inputfile checksumfile
    exit 1
}


func_exit()
{
  $DEBUG && echo $1
  (
    flock -x -w 10 200 || exit 1
    echo "$(date -Iseconds) $1" >>$LOG
  ) 200>$LOCK
  exit $2
}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
. $DIR/compress_conf.sh



INPUT_FILE=$(trim $1)
CHECKSUM_FILE=$(trim $2)

if [ -z "$INPUT_FILE" ]; then
      usage "No input file specified."
fi


if [ -z "$CHECKSUM_FILE" ]; then
      usage "No checksum file specified."
fi

if ! [ -f $INPUT_FILE ]; then
     func_exit "No such file $INPUT_FILE" 2
fi





