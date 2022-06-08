TS := $(shell date +%Y%m%d-%H%M%S)

VERSION := $(shell mvn -f proxy -q -Dexec.executable=echo -Dexec.args='$${project.version}' --non-recursive exec:exec)
ARTIFACT_ID := $(shell mvn -f proxy -q -Dexec.executable=echo -Dexec.args='$${project.artifactId}' --non-recursive exec:exec)
REVISION ?= ${TS}
USER ?= $(LOGNAME)
REPO ?= proxy-dev
PACKAGECLOUD_USER ?= wavefront
PACKAGECLOUD_REPO ?= proxy-next

DOCKER_TAG ?= ${VERSION}_${REVISION}

out = $(shell pwd)/out
$(shell mkdir -p $(out))

.info:
	@echo "\n----------\nBuilding Proxy ${VERSION}\nDocker tag: $(USER)/$(REPO):$(DOCKER_TAG) \n----------\n"

jenkins: .info build-jar build-linux push-linux docker-multi-arch clean

#####
# Build Proxy jar file
#####
build-jar: .info
	mvn -f proxy --batch-mode clean package ${MVN_ARGS}
	cp proxy/target/${ARTIFACT_ID}-${VERSION}-spring-boot.jar ${out}

#####
# Build single docker image
#####
docker: .info .cp-docker
	docker build -t $(USER)/$(REPO):$(DOCKER_TAG) docker/

#####
# Build multi arch (amd64 & arm64) docker images
#####
docker-multi-arch: .info .cp-docker
	docker buildx create --use
	docker buildx build --platform linux/amd64,linux/arm64 -t $(USER)/$(REPO):$(DOCKER_TAG) --push docker/

docker-multi-arch-with-latest-tag: .info .cp-docker
	docker buildx create --use
	docker buildx build --platform linux/amd64,linux/arm64 -t $(USER)/$(REPO):$(DOCKER_TAG) -t $(USER)/$(REPO):latest --push docker/

#####
# Build rep & deb packages
#####
build-linux: .info .prepare-builder .cp-linux
	docker run -v $(shell pwd)/:/proxy proxy-linux-builder /proxy/pkg/build.sh ${VERSION} ${REVISION}
	
#####
# Push rep & deb packages
#####
push-linux: .info .prepare-builder
	docker run -v $(shell pwd)/:/proxy proxy-linux-builder /proxy/pkg/upload_to_packagecloud.sh ${PACKAGECLOUD_USER}/${PACKAGECLOUD_REPO} /proxy/pkg/package_cloud.conf /proxy/out

#####
# Package for Macos
#####
pack-macos:
	cp ${out}/${ARTIFACT_ID}-${VERSION}-spring-boot.jar macos/wavefront-proxy.jar
	cd macos && zip ${out}/wfproxy_${VERSION}_${REVISION}.zip *
	unzip -t ${out}/wfproxy_${VERSION}_${REVISION}.zip


#####
# Run Proxy complex Tests
#####
tests: .info .cp-docker
	$(MAKE) -C tests/chain-checking all

.prepare-builder:
	docker build -t proxy-linux-builder pkg/

.cp-docker:
	cp ${out}/${ARTIFACT_ID}-${VERSION}-spring-boot.jar docker/wavefront-proxy.jar
	${MAKE} .set_package JAR=docker/wavefront-proxy.jar PKG=docker

.cp-linux:
	cp ${out}/${ARTIFACT_ID}-${VERSION}-spring-boot.jar pkg/wavefront-proxy.jar
	${MAKE} .set_package JAR=pkg/wavefront-proxy.jar PKG=linux_rpm_deb

clean:
	docker buildx prune -a -f	

.set_package:
	jar -xvf ${JAR} BOOT-INF/classes/build.properties
	sed 's/\(build.package=\).*/\1${PKG}/' BOOT-INF/classes/build.properties > build.tmp && mv build.tmp BOOT-INF/classes/build.properties
	jar -uvf ${JAR} BOOT-INF/classes/build.properties
	rm BOOT-INF/classes/build.properties
