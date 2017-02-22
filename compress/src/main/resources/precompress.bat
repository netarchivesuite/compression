Set PACKAGE=dk.nationalbiblioteket.netarkivet.compression.precompression
Set CLASS=PreCompressor
Set JARDIR=%~dp0\..\lib
Set CONFDIR=%~dp0\..\config
java -classpath "%JARDIR%/*" -Dconfig="%CONFDIR%/precompress.conf" %PACKAGE%.%CLASS% %1
