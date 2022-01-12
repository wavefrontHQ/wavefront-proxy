TS = $(shell date +%Y%m%d-%H%M%S)

VERSION = $(shell mvn -f proxy -q -Dexec.executable=echo -Dexec.args='$${project.version}' --non-recursive exec:exec)
REVISION ?= ${TS}
FULLVERSION = ${VERSION}_${REVISION}
USER ?= $(LOGNAME)
REPO ?= proxy-dev

DOCKER_TAG = $(USER)/$(REPO):${FULLVERSION}
DOCKER_TAG_RHEL = $(USER)/$(REPO):RHEL_${FULLVERSION}

out = $(shell pwd)/out
$(shell mkdir -p $(out))

info:
	@echo "\n----------\nBuilding Proxy ${FULLVERSION}\nDocker tag: ${DOCKER_TAG}\nDocker tag RHEL: ${DOCKER_TAG_RHEL}\n----------\n"

jenkins: info build-jar build-linux push-linux docker-multi-arch docker-rhel docker-rhel-push clean

#####
# Build Proxy jar file
#####
# !!! REMOVE `-DskipTests`
build-jar: info
	mvn -f proxy --batch-mode package -DskipTests 
	cp proxy/target/proxy-*-uber.jar ${out}/wavefront-proxy.jar

#####
# Build single docker image
#####
docker: info cp-docker
	docker build -t $(DOCKER_TAG) docker/


#####
# Build multi arch (amd64 & arm64) docker images
#####
docker-multi-arch: info cp-docker
	docker buildx create --use
	docker buildx build --platform linux/amd64,linux/arm64 -t $(DOCKER_TAG) --push docker/


#####
# Build rep & deb packages
#####
build-linux: info prepare-builder cp-linux
	docker run -v $(shell pwd)/:/proxy proxy-linux-builder /proxy/pkg/build.sh ${VERSION} ${REVISION}
	
#####
# Push rep & deb packages
#####
# replace proxy-next
push-linux: info prepare-builder
	docker run -v $(shell pwd)/:/proxy proxy-linux-builder /proxy/pkg/upload_to_packagecloud.sh wavefront/proxy-next /proxy/pkg/package_cloud.conf /proxy/out

prepare-builder:
	docker build -t proxy-linux-builder pkg/

cp-docker:
	cp ${out}/wavefront-proxy.jar docker

cp-linux:
	cp ${out}/wavefront-proxy.jar pkg

clean:
	docker buildx prune -a -f

