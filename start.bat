@echo off
setlocal EnableDelayedExpansion

REM === CONFIGURAÇÕES ===
set IMAGE_NAME=ramonbarbosdev/syncdb
set JAR_NAME=syncdb-0.0.1-SNAPSHOT.jar

echo ========================================
echo Compilando com Maven...
echo ========================================
call mvn clean package -DskipTests

if %ERRORLEVEL% NEQ 0 (
    echo Erro ao compilar com Maven.
    pause
    exit /b %ERRORLEVEL%
)

echo ========================================
echo Verificando se o JAR foi gerado...
echo ========================================
if not exist "target\%JAR_NAME%" (
    echo Arquivo %JAR_NAME% não encontrado!
    pause
    exit /b 1
)

echo JAR encontrado: target\%JAR_NAME%

echo ========================================
echo Criando imagem Docker...
echo ========================================
docker build -t %IMAGE_NAME%:latest .

if %ERRORLEVEL% NEQ 0 (
    echo Erro ao criar imagem Docker.
    pause
    exit /b %ERRORLEVEL%
)

echo ========================================
echo Enviando imagem para o Docker Hub...
echo ========================================
docker push %IMAGE_NAME%:latest

if %ERRORLEVEL% NEQ 0 (
    echo Erro ao enviar imagem para o Docker Hub.
    pause
    exit /b %ERRORLEVEL%
)

echo ========================================
echo Subindo containers com Docker Compose...
echo ========================================
docker-compose down
docker-compose up --build --force-recreate --remove-orphans

echo ========================================
echo Tudo pronto! Acesse http://localhost:8080
echo ========================================
pause
