IF [%2]==[] GOTO NO_ARGUMENT

cscript %~dp0\split.vbs %1 %2
Set PACKAGE=dk.nationalbiblioteket.netarkivet.compression.precompression
Set CLASS=PreCompressor
Set JARDIR=%~dp0\..\lib
Set CONFDIR=%~dp0\..\config

for %%f in (split_output_*)  do (
   java -classpath "%JARDIR%/*" -Dconfig="%CONFDIR%/precompress.conf" %PACKAGE%.%CLASS% %%f
   del %%f
)

GOTO DONE

:NO_ARGUMENT
ECHO "Usage: precompress.bat <corpus_file> <chunksize>"
exit /B 1

:DONE