TS = $(shell date +%Y%m%d%H%M%S)
REPO ?= $(LOGNAME)
NAME ?= proxy-dev
TAG ?= $(TS)

build-jar:
	cd ../ && mvn package
	cp ../proxy/target/proxy-*-uber.jar wavefront-proxy.jar

build-docker:
	docker build -t $(REPO)/$(NAME):$(TAG) .

build-multi-arch-docker:
	docker buildx create --use
	docker buildx build --platform linux/amd64,linux/arm64 -t $(REPO)/$(NAME):$(TAG) --push .

clean:
	cd ../ && mvn clean
	rm wavefront-proxy.jar
