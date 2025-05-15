@echo off

docker --version >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo "Docker não encontrado. Certifique-se de que o Docker esteja instalado."
    pause
    exit /b
)

docker-compose --version >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo "Docker Compose não encontrado. Certifique-se de que o Docker Compose esteja instalado."
    pause
    exit /b
)

set REPOSITORY=ramonbarbosdev/syncdb

set IMAGE_NAME=syncdb
set TAG=latest

echo "Derrubando os containers existentes..."
docker-compose down

echo "Recriando e atualizando os containers..."
docker-compose up --build --force-recreate --remove-orphans

echo "Construindo a imagem Docker..."
docker build -t %REPOSITORY%/%IMAGE_NAME%:%TAG% .

echo "Enviando a imagem para o Docker Hub..."
docker push %REPOSITORY%/%IMAGE_NAME%:%TAG%

echo "Processo concluído!"
pause
