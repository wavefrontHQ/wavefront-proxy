TS = $(shell date +%Y%m%d%H%M%S)
REPO ?= $(LOGNAME)
NAME ?= proxy-dev
TAG ?= $(TS)

all: clean build-jar make-docker

build-jar: clean
	mvn -f proxy --batch-mode package -DskipTests
	cp proxy/target/proxy-*-uber.jar wavefront-proxy.jar

make-docker: build-jar
	cd docker && $(MAKE) build-multi-arch-docker
	
clean: 
	mvn -f proxy --batch-mode clean
	rm -rf wavefront-proxy.jar