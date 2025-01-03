@ECHO OFF

title Guide Execute Tools v.2.3.7.6
setlocal EnableDelayedExpansion

::set/p "MinusDay= Input Periode Tarik Data (Input 0 jika hari H, input 1 jika H-1) : "

:// Minus days is the number of days to subtract from the CURRENT DAY i.e. 2 for minus 2 days or 99999 for minus 99999 days from when it's run
SET MinusDay=1

:: This calls the temp vbs script routine that will be used to set YYYY-MM-DD values for the subtracted days date you specify
CALL :DynamicVBSScriptBuild

FOR /F "TOKENS=*" %%A IN ('cscript//nologo "%YYYYTmpVBS%"') DO SET YYYY=%%A
FOR /F "TOKENS=*" %%A IN ('cscript//nologo "%MMTmpVBS%"') DO SET MM=%%A
FOR /F "TOKENS=*" %%A IN ('cscript//nologo "%DDTmpVBS%"') DO SET DD=%%A

::// Set your web server log file path in the below variable
SET WebServerLogPath=C:\WebServer\Logs

::// Set web server log file name where YYYY MM DD variables are set to the values after the day numbers setup above are subtracted
SET YYYY=%YYYY%
SET MM=%MM%
SET DD=%DD%
SET /A DD2=%DD% -1

IF %DD2% LSS 10 SET DD2=0%DD2%


ECHO Periode Tarik File : %yyyy:~2,2%%MM%%DD%

set hr2=?R%yyyy:~2,2%%MM%%DD2%.???

set formathr=?R%yyyy:~2,2%%MM%%DD%.???

echo %hr2%
echo %formathr%

:: File XML yang akan dimodifikasi
SET "file=iris-to-z.ffs_batch"

:: Ganti bagian tertentu dari file XML dengan tanggal h-1
CALL :ReplaceInFile %file% "%hr2%" "%formathr%"
GOTO :EOF

:ReplaceInFile
SET "file=%~1"
SET "search=%~2"
SET "replace=%~3"
SET "tempfile=%file%.tmp"

REM Buka file sumber untuk dibaca
< "%file%" (
  REM Buka file sementara untuk menulis
  (
    REM Loop melalui setiap baris di file sumber
    FOR /F "delims=" %%A IN ('findstr /n "^"') DO (
      REM Ambil baris saat ini tanpa nomor baris
      SET "line=%%A"
      SETLOCAL EnableDelayedExpansion
      REM Buang nomor baris dari baris saat ini
      SET "line=!line:*:=!"
      REM Ganti teks yang dicari dengan teks pengganti di baris saat ini
      SET "line=!line:%search%=%replace%!"
      REM Tulis baris yang sudah dimodifikasi ke file sementara
      ECHO(!line!
      ENDLOCAL
    )
  ) > "%tempfile%"
)

REM Hapus file asli
DEL "%file%" >NUL

REM Ubah nama file sementara menjadi nama file asli
REN "%tempfile%" "%file%"

GOTO :EOF



:DynamicVBSScriptBuild
SET YYYYTmpVBS=%temp%\~tmp_yyyy.vbs
SET MMTmpVBS=%temp%\~tmp_mm.vbs
SET DDTmpVBS=%temp%\~tmp_dd.vbs
IF EXIST "%YYYYTmpVBS%" DEL /Q /F "%YYYYTmpVBS%"
IF EXIST "%MMTmpVBS%" DEL /Q /F "%MMTmpVBS%"
IF EXIST "%DDTmpVBS%" DEL /Q /F "%DDTmpVBS%"
ECHO dt = DateAdd("d",-%MinusDay%,date) >> "%YYYYTmpVBS%"
ECHO yyyy = Year(dt)                    >> "%YYYYTmpVBS%"
ECHO WScript.Echo yyyy                  >> "%YYYYTmpVBS%"
ECHO dt = DateAdd("d",-%MinusDay%,date) >> "%MMTmpVBS%"
ECHO mm = Right("0" ^& Month(dt),2)     >> "%MMTmpVBS%"
ECHO WScript.Echo mm                    >> "%MMTmpVBS%"
ECHO dt = DateAdd("d",-%MinusDay%,date) >> "%DDTmpVBS%"
ECHO dd = Right("0" ^& Day(dt),2)       >> "%DDTmpVBS%"
ECHO WScript.Echo dd                    >> "%DDTmpVBS%"
GOTO :EOF