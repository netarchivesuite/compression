#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PACKAGE=dk.nationalbiblioteket.netarkivet.compression.precompression
CLASS=PreCompressor
java -cp "$DIR/../lib/"'*' -Dlogback.configurationFile=$DIR/../config/precompress.logback.xml -Dconfig=$DIR/../config/precompress.conf $PACKAGE.$CLASS $1 $2
