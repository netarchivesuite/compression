Set PACKAGE=dk.nationalbiblioteket.netarkivet.compression.precompression

Set CLASS=PreCompressor


PUSHD "%~dp0" >NUL && SET root=%CD% && POPD >NUL
java -classpath "%ROOT%\../lib/*" -Dconfig="../config/precompress.conf" %PACKAGE%.%CLASS% %1
