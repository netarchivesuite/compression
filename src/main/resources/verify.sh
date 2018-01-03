#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PACKAGE=dk.nationalbiblioteket.netarkivet.compression.precompression
CLASS=VerifyPrecompressionOnFile
java -cp "$DIR/../lib/"'*' -Dlogback.configurationFile=$DIR/../config/precompress.logback.xml -Dconfig=$DIR/../config/precompress.conf $PACKAGE.$CLASS $1
