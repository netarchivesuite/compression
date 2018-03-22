IF [%2]==[] GOTO NO_ARGUMENT

cscript %~dp0\split.vbs %1 %2

Set PACKAGE=dk.nationalbiblioteket.netarkivet.compression.compression
Set CLASS=Compressor
Set JARDIR=%~dp0\..\lib
Set CONFDIR=%~dp0\..\config

for %%f in (split_output_*)  do (
   java -classpath "%JARDIR%/*" -Dconfig="%CONFDIR%/compress.conf" %PACKAGE%.%CLASS% %%f %3
   del %%f
   ECHO "Finished processing corpus file %%f"
)

ECHO "Finished all processing"

GOTO DONE

:NO_ARGUMENT
ECHO "Usage: compress.bat <corpus_file> <chunksize> <blacklistfile>"
exit /B 1

:DONE