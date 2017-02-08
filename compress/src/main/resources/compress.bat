Set PACKAGE=dk.nationalbiblioteket.netarkivet.compression.compression

Set CLASS=Compressor


PUSHD "%~dp0" >NUL && SET root=%CD% && POPD >NUL
java -classpath "%ROOT%\../lib/*" -Dconfig="%ROOT%\../config/precompress.conf" %PACKAGE%.%CLASS% %1
