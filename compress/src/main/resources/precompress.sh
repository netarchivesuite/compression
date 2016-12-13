#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PACKAGE=dk.nationalbiblioteket.netarkivet.compression.precompression
CLASS=PreCompressor
java -cp "$DIR/../lib/"'*' -Dconfig=$DIR/../config/precompress.conf $PACKAGE.$CLASS $1