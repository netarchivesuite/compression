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

if ! [ -f $CHECKSUM_FILE ]; then
     func_exit "No such file $CHECKSUM_FILE" 2
fi

BASEFILE=$(basename $INPUT_FILE)
## How badly does the next line scale?
MD5_EXPECTED=$(grep $BASEFILE $CHECKSUM_FILE |cut -d'#' -f 3)
MD5_EXPECTED=$(trim $MD5_EXPECTED)
if [ -z "$MD5_EXPECTED" ]; then
     func_exit "No target checksum for $BASEFILE in $CHECKSUM_FILE" 3
fi




$JWAT_DIR/jwattools.sh --verify compress $JWAT_COMPRESSION $INPUT_FILE
OUTPUT_FILE="$INPUT_FILE".gz

if ! [ -f $OUTPUT_FILE ]; then
     func_exit "Output file $OUTPUT_FILE not generated." 3
fi

MD5_ACTUAL=$( md5sum $OUTPUT_FILE |cut -d' ' -f 1)

if [ "$MD5_ACTUAL" == "$MD5_EXPECTED" ]; then
(
  flock -x -w 10 202 || exit 1
  ## rm $INPUT_FILE
  echo "Created $OUTPUT_FILE . Deleted $INPUT_FILE ." >>$LOG
) 202>$LOCK
else
(
  flock -x -w 10 202 || exit 1
  echo "Created $OUTPUT_FILE . Did not delete $INPUT_FILE because of md5sum mismatch." >>$LOG
) 202>$LOCK
fi
