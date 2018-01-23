#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PACKAGE=dk.nationalbiblioteket.netarkivet.compression.compression
CLASS=Compressor
java -cp "$DIR/../lib/"'*' -Dlogback.configurationFile=$DIR/../config/compress.logback.xml -Dconfig=$DIR/../config/compress.conf $PACKAGE.$CLASS $1 $2
