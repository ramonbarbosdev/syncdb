@echo off
docker-compose down

docker build -t backend-syncdb .

docker-compose up --build --force-recreate --remove-orphans 

docker run -p 8080:8080 --name backend-syncdb backend-syncdb
pause
