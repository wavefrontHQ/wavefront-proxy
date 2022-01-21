TS = $(shell date +%Y%m%d-%H%M%S)

VERSION = $(shell mvn -f proxy -q -Dexec.executable=echo -Dexec.args='$${project.version}' --non-recursive exec:exec)
REVISION ?= ${TS}
FULLVERSION = ${VERSION}_${REVISION}
USER ?= $(LOGNAME)
REPO ?= proxy-dev
PACKAGECLOUD_USER ?= wavefront

DOCKER_TAG = $(USER)/$(REPO):${FULLVERSION}

out = $(shell pwd)/out
$(shell mkdir -p $(out))

info:
	@echo "\n----------\nBuilding Proxy ${FULLVERSION}\nDocker tag: ${DOCKER_TAG}\n----------\n"

jenkins: info build-jar build-linux push-linux docker-multi-arch clean

#####
# Build Proxy jar file
#####
# !!! REMOVE `-DskipTests`
build-jar: info
	mvn -f proxy --batch-mode package -DskipTests 
	cp proxy/target/proxy-${VERSION}-uber.jar ${out}

#####
# Build single docker image
#####
docker: info .cp-docker
	docker build -t $(DOCKER_TAG) docker/


#####
# Build multi arch (amd64 & arm64) docker images
#####
docker-multi-arch: info .cp-docker
	docker buildx create --use
	docker buildx build --platform linux/amd64,linux/arm64 -t $(DOCKER_TAG) --push docker/


#####
# Build rep & deb packages
#####
build-linux: info prepare-builder .cp-linux
	docker run -v $(shell pwd)/:/proxy proxy-linux-builder /proxy/pkg/build.sh ${VERSION} ${REVISION}
	
#####
# Push rep & deb packages
#####
push-linux: info prepare-builder
	docker run -v $(shell pwd)/:/proxy proxy-linux-builder /proxy/pkg/upload_to_packagecloud.sh ${PACKAGECLOUD_USER}/${REPO} /proxy/pkg/package_cloud.conf /proxy/out

prepare-builder:
	docker build -t proxy-linux-builder pkg/

.cp-docker:
	cp ${out}/proxy-${VERSION}-uber.jar docker/wavefront-proxy.jar
	${MAKE} .set_package JAR=docker/wavefront-proxy.jar PKG=docker

.cp-linux:
	cp ${out}/proxy-${VERSION}-uber.jar pkg/wavefront-proxy.jar
	${MAKE} .set_package JAR=docker/wavefront-proxy.jar PKG=linux_rpm_deb

clean:
	docker buildx prune -a -f

.set_package:
	jar -xvf ${JAR} build.properties
	sed -i '' 's/\(build.package=\).*/\1${PKG}/' build.properties
	jar -uvf ${JAR} build.properties
	rm build.properties
