export LOCAL_UID ?= $(shell id -u)
export LOCAL_GID ?= $(shell id -g)

.PHONY: help build run report test logs ps down clean
help:
	@echo "make run     Build, capture requests, run v1 and v2, and verify Iceberg results"
	@echo "make report  Read the last experiment from Iceberg and repeat its checks"
	@echo "make test    Run isolated Python tests (no services required)"
	@echo "make down    Stop the example and keep its data"
	@echo "make clean   Delete this example's containers and named volumes"
build:
	docker compose build flink-jobmanager demo web-v1 web-v2 iceberg-rest
run: build
	docker compose up -d --wait iggy web-v1 web-v2 minio iceberg-rest flink-jobmanager flink-taskmanager
	docker compose run --rm --no-deps demo
report:
	docker compose run --rm --no-deps demo python main.py report
test:
	docker compose build demo
	docker compose run --rm --no-deps demo python -m pytest -q -o cache_dir=/tmp/pytest-cache
logs:
	docker compose logs --tail=100 -f
ps:
	docker compose ps -a
down:
	docker compose down --remove-orphans
clean:
	docker compose down -v --remove-orphans
