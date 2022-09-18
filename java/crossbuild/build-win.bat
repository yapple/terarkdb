:: install git, java 8, maven, visual studio community 15 (2017)

set MSBUILD=C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\MSBuild\15.0\Bin\MSBuild.exe

if exist build rd /s /q build
if exist libterarkdbjni-win64.dll del libterarkdbjni-win64.dll
mkdir build && cd build

cmake -G "Visual Studio 15 Win64" -DWITH_JNI=1 ..

"%MSBUILD%" rocksdb.sln /p:Configuration=Release /m

cd ..

copy build\java\Release\rocksdbjni-shared.dll libterarkdbjni-win64.dll
echo Result is in libterarkdbjni-win64.dll