#!/bin/bash

#
# Some variables which should be replaced by settings in a .conf file or commandline options
#
## JWAT_DIR=$HOME/projects/jwat-tools/target/jwat-tools-0.6.3-SNAPSHOT
OUTPUT_ROOT_DIR=.
JWAT_DIR=/netarkiv-devel/jwat
OUTPUT_DIR=.
DELETE=0
DEBUG=true
LOG=./compression.log
JWAT_COMPRESSION=-9
DEPTH=4
CHECKSUM_FILE=checksum_CS.md5


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

# Find the output directory and create it if necessary
JOBNR=$(basename $INPUT_FILE|cut -d '-' -f 1|xargs printf "%0${DEPTH}d")
$DEBUG && echo Padded Job-Number $JOBNR
OUTPUT_DIR=$OUTPUT_ROOT_DIR
for i in $(seq 1 ${#JOBNR})
do
    OUTPUT_DIR=$OUTPUT_DIR/${JOBNR:i-1:1}
done
$DEBUG && echo Output dir is $OUTPUT_DIR
mkdir -p $OUTPUT_DIR



# Calculate the sha1 for the input file
OSHA1=$(sha1sum $1 |cut -d ' ' -f 1)
$DEBUG && echo "sha1 $OSHA1 for $INPUT_FILE"

# Generate the output file name
OUTPUT_FILE=$OUTPUT_DIR/$(basename "$1").gz
$DEBUG && echo Compressing to $OUTPUT_FILE.

# Create the compressed file
$JWAT_DIR/jwattools.sh --verify compress $JWAT_COMPRESSION $INPUT_FILE
mv ${INPUT_FILE}.gz $OUTPUT_FILE
if [ $? -eq 0 ];  then
     $DEBUG && echo Created compressed file $OUTPUT_FILE from $INPUT_FILE
else
     func_exit "Could not create compressed file $OUTPUT_FILE from $INPUT_FILE." 4
fi

# Check that the file can be reinflated
TEMP_FILE=$(mktemp)
gunzip -c $OUTPUT_FILE --to-stdout > $TEMP_FILE
NSHA1=$(sha1sum $TEMP_FILE |cut -d ' ' -f 1)
if [ "$NSHA1" != "$OSHA1" ]; then
   func_exit "SHA1 mismatch on decompression between $INPUT_FILE and $TEMP_FILE."
else
   $DEBUG && echo "Checksum match confirmed for $INPUT_FILE."
   rm $TEMP_FILE
fi

# Calculate the md5 for the output file and store
echo $(basename $OUTPUT_FILE)"##"$(md5sum $OUTPUT_FILE | cut -d' ' -f 1)   >>$CHECKSUM_FILE

# Create a cdx file for the input file
OCDX=$OUTPUT_DIR/$(basename "$1").cdx
$DEBUG && echo Creating cdx $OCDX
$JWAT_DIR/jwattools.sh cdx -o $OCDX $INPUT_FILE
if [ $? -ne 0 ]; then
    func_exit "Error creating cdx file $OCDX from $INPUT_FILE." 3
fi

# Create a cdx file for the output file
NCDX=${OUTPUT_FILE}.cdx
$JWAT_DIR/jwattools.sh cdx -o $NCDX $OUTPUT_FILE
if [ $? -ne 0 ]; then
  func_exit "Error creating cdx file $NCDX from $OUTPUT_FILE." 5
else
  $DEBUG && echo "Deleting output file $OUTPUT_FILE."
  rm $OUTPUT_FILE
fi

# Compare the line count of the two cdx files
LOCDX=$(wc -l $OCDX|cut -d ' ' -f 1)
LNCDX=$(wc -l $NCDX|cut -d ' ' -f 1)
if [ $LOCDX -ne $LNCDX ]; then
    func_exit "Output and input files have different numbers of records ($LNCDX vs. $LOCDX)." 6
fi

# Generate the i-file name
IFILE=$OUTPUT_DIR/$(basename "$INPUT_FILE").ifile.cdx
$DEBUG && echo "Creating ifile $IFILE."
rm -f $IFILE

# Loop over the two cdx files to create the lookup cdx
while read ocdx_line <&3 && read ncdx_line <&4; do
if [[ ! "$ocdx_line" =~ CDX ]]; then
     echo $(echo $ocdx_line| rev |cut -d ' ' -f 2|rev )" "$(echo $ncdx_line|rev|cut -d ' ' -f 2|rev )" "$(echo $ncdx_line|cut -d ' ' -f 2 ) >>$IFILE
fi
done 3<$OCDX 4<$NCDX
if [ $? -ne 0 ]; then
   func_exit "Error creating  ifile $IFILE." 8
else
   $DEBUG && echo "Cleaning up."
   rm $NCDX $OCDX
fi