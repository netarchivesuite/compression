#!/usr/bin/env bash

OUTPUT_ROOT_DIR=/netarkiv-devel/output
JWAT_DIR=/netarkiv-devel/jwat
DELETE=0
DEBUG=true
LOG=$OUTPUT_ROOT_DIR/compression.log
JWAT_COMPRESSION=-9
DEPTH=4
CHECKSUM_FILE=$OUTPUT_ROOT_DIR/checksum_CS.md5
LOCK=$OUTPUT_ROOT_DIR/log.lock
CSLOCK=$OUTPUT_ROOT_DIR/cslock.lock
TMPDIR=/netarkiv-devel/tmp