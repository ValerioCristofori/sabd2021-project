build:
	mvn clean package
up:
	docker network create spark-net
	docker-compose -f docker-compose.yml up -d
down:
	docker-compose -f docker-compose.yml down
	docker network rm spark-net
app:
	docker-compose -f docker-compose.yml build
	docker-compose -f docker-compose.yml up
