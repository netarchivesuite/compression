#!/bin/bash

#
# Utility script to run on kb-prod-udv-001 to find the input list with all the filepaths to be precompressed.
#

envname=COMPRESSIONCORPUS

dirs=(  "d:\\bitarkiv_1\\${envname}\\filedir" #
"e:\\bitarkiv_2\\${envname}\\filedir"  #
"f:\\bitarkiv_3\\${envname}\\filedir"   #
)

for dir in "${dirs[@]}"
do

ssh ba-test@kb-test-bar-014.bitarkiv.kb.dk "dir /B $dir" |xargs -n 1 echo $dir|sed 's! !\\!'

done