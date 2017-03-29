''This simple VBScript spilts large text files into multiple files

Dim  Counter
InputFile = WScript.Arguments.Item(0)
Const OutputFile = "split_output"
RecordSize = CInt(WScript.Arguments.Item(1))
Const ForReading = 1
Const ForWriting = 2
Const ForAppending = 8
Set objFSO = CreateObject("Scripting.FileSystemObject")
Set objTextFile = objFSO.OpenTextFile (InputFile, ForReading)
Counter = 0
FileCounter = 0
Set objOutTextFile = Nothing

Do Until objTextFile.AtEndOfStream
    if Counter = 0 Or Counter = RecordSize Then
        Counter = 0
        FileCounter = FileCounter + 1
        if Not objOutTextFile is Nothing then objOutTextFile.Close
        Set objOutTextFile = objFSO.OpenTextFile( OutputFile & "_" & FileCounter & ".csv", ForWriting, True)
    end if
    strNextLine = objTextFile.Readline
    objOutTextFile.WriteLine(strNextLine)
    Counter = Counter + 1
    Wscript.echo Counter
Loop
objTextFile.Close
objOutTextFile.Close