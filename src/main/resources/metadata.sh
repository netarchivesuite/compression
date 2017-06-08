#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PACKAGE=dk.nationalbiblioteket.netarkivet.compression.metadata
CLASS=MetadatafileGenerator
java -Dlogback.configurationFile=$DIR/../config/metadata.logback.xml -cp  "$DIR/../lib/"'*' -Dconfig=$DIR/../config/metadata.conf $PACKAGE.$CLASS $1