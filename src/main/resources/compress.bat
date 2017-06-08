Set PACKAGE=dk.nationalbiblioteket.netarkivet.compression.compression
Set CLASS=Compressor

Set JARDIR=%~dp0\..\lib
Set CONFDIR=%~dp0\..\config

java -classpath "%JARDIR%/*" -Dconfig="%CONFDIR%/compress.conf" %PACKAGE%.%CLASS% %1

