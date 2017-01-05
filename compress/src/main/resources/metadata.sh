#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PACKAGE=dk.nationalbiblioteket.netarkivet.compression.metadata
CLASS=MetadatafileGenerator
java -cp "$DIR/../lib/"'*' -Dconfig=$DIR/../config/metadata.conf $PACKAGE.$CLASS $1