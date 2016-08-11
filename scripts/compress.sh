#!/bin/bash

#
# Some variables which should be replaced by settings in a .conf file or commandline options
#
JWAT_DIR=$HOME/jwat
OUTPUT_DIR=.
DELETE=0
DEBUG=true
LOG=./compression.log
JWAT_COMPRESSION=-9

func_exit()
{
$DEBUG && echo $1
echo $1 >>$LOG
exit $2
}

INPUT_FILE=$1
# Check that input file
# a) exists
# b) ends in .arc or .warc
# c) is not a metadata file
if ! [ -f $1 ]; then
    func_exit "$1 doesn't exist." 2
fi

# Create a cdx file for the input file

OCDX=$OUTPUT_DIR/$(basename "$1").cdx
$DEBUG && echo Creating cdx $OCDX
$JWAT_DIR/jwattools.sh cdx -o $OCDX $INPUT_FILE
if [ $? -ne 0 ]; then
    func_exit "Error creating cdx file $OCDX from $INPUT_FILE." 3
fi

# Calculate the sha1 for the input file
OSHA1=$(sha1sum $1 |cut -d ' ' -f 1)
$DEBUG && echo "sha1 $OSHA1 for $INPUT_FILE"

# Generate the output file name
OUTPUT_FILE=$OUTPUT_DIR/$(basename "$1").gz
$DEBUG && echo Compressing to $OUTPUT_FILE.

# Create the compressed file
$JWAT_DIR/jwattools.sh compress $JWAT_COMPRESSION $INPUT_FILE
mv ${INPUT_FILE}.gz $OUTPUT_FILE
if [ $? -eq 0 ];  then
     $DEBUG && echo Created compressed file $OUTPUT_FILE from $INPUT_FILE
else
     func_exit "Could not create compressed file $OUTPUT_FILE from $INPUT_FILE." 4
fi
# Calculate the sha1 for the output file and store
SHA_FILE=$OUTPUT_DIR/$(basename "$1").sha1
sha1sum $OUTPUT_FILE > $SHA_FILE

# Create a cdx file for the output file
NCDX=${OUTPUT_FILE}.cdx
$JWAT_DIR/jwattools.sh cdx -o $NCDX $OUTPUT_FILE
if [ $? -ne 0 ]; then
  func_exit "Error creating cdx file $NCDX from $OUTPUT_FILE." 5
fi

# Compare the line count of the two cdx files
LOCDX=$(wc -l $OCDX|cut -d ' ' -f 1)
LNCDX=$(wc -l $NCDX|cut -d ' ' -f 1)
if [ $LOCDX -ne $LNCDX ]; then
    func_exit "Output and input files have different numbers of records ($LNCDX vs. $LOCDX)." 6
fi

# Create a temporary decompressed file

# Compare the decompressed file with the original

# Delete the original

# Loop over the two cdx files to create the lookup cdx

