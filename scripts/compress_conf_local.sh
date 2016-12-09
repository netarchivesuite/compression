#!/usr/bin/env bash

OUTPUT_ROOT_DIR=${HOME}/temp/output
JWAT_DIR=${HOME}/temp/jwat-tools-0.6.3-SNAPSHOT
DELETE=0
DEBUG=true
LOG=$OUTPUT_ROOT_DIR/compression.log
JWAT_COMPRESSION=-9
DEPTH=4
CHECKSUM_FILE=$OUTPUT_ROOT_DIR/checksum_CS.md5
LOCK=$OUTPUT_ROOT_DIR/compression.log.lock
CSLOCK=$OUTPUT_ROOT_DIR/cslock.lock
TMPDIR=/tmp