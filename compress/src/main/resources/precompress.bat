cscript split.vbs %1 %2
Set PACKAGE=dk.nationalbiblioteket.netarkivet.compression.precompression
Set CLASS=PreCompressor
Set JARDIR=%~dp0\..\lib
Set CONFDIR=%~dp0\..\config

for %%F in (split_output_*)  do (
   java -classpath "%JARDIR%/*" -Dconfig="%CONFDIR%/precompress.conf" %PACKAGE%.%CLASS% %%f
   del %%f
)