#!/bin/bash

##
## Script using Bitvise ssh to distribute multiple runs of precompress.bat to machines with identical setup
##

##
## The number of files to precompress in each windows process. This determines how much tmp space is used, because
## Windows only releases the files and deletes them when the process ends
##
CHUNKSIZE=2

##
## Path to the bvRun executable from Bitvise
##
BVRUN='C:\Program Files\Bitvise WinSSHD\bvRun.exe'

##
## Current working directory for the precompress process
##
CWD='C:\Users\ba-devel.BITARKIV\compression'

##
## Location of precompress.bat relative to CWD
##
CMD='bin\precompress.bat'

##
## Location of the input file listing the files to be precompressed, relative to CWD
##
CORPUS=corpus.txt

##
## Location for console output (stdout and stderr) relative to CWD
##
OUTPUT=out.txt

ssh ba-devel@kb-test-bar-014.bitarkiv.kb.dk 'cmd /c ""'"${BVRUN}"'" -brj -cwd="'"${CWD}"'" -cmd="cmd /c '"${CMD}"' '"${CORPUS}"' '"${CHUNKSIZE}"' > '"${OUTPUT}"' 2>&1""'
ssh ba-devel@kb-test-bar-015.bitarkiv.kb.dk 'cmd /c ""'"${BVRUN}"'" -brj -cwd="'"${CWD}"'" -cmd="cmd /c '"${CMD}"' '"${CORPUS}"' '"${CHUNKSIZE}"' > '"${OUTPUT}"' 2>&1""'
ssh ba-devel@kb-test-bar-016.bitarkiv.kb.dk 'cmd /c ""'"${BVRUN}"'" -brj -cwd="'"${CWD}"'" -cmd="cmd /c '"${CMD}"' '"${CORPUS}"' '"${CHUNKSIZE}"' > '"${OUTPUT}"' 2>&1""'
.
.
.
etc.