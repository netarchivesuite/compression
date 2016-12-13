:: gets the parent directory of the bin directory
PUSHD "%~dp0" >NUL && SET root=%CD% && POPD >NUL
Set PACKAGE=dk.nationalbiblioteket.netarkivet.compression.precompression
Set CLASS=PreCompressor

java -cp "%ROOT%\lib\"'*' -Dconfig=%ROOT%\config\precompress.conf %PACKAGE%.%CLASS% %1