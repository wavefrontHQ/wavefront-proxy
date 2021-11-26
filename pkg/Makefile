TS = $(shell date +%Y%m%d%H%M%S)
VERSION = $(shell mvn -f ../proxy -q -Dexec.executable=echo -Dexec.args='$${project.version}' --non-recursive exec:exec)
REPO ?= wavefront/proxy-next

all: build-jar prepare-builder build-packages

build-jar:
	cd ../ && mvn -f proxy --batch-mode package
	cp ../proxy/target/proxy-*-uber.jar wavefront-proxy.jar

prepare-builder:
	docker build -t builder .

build-packages:
	docker run -v $(shell pwd)/../:/proxy builder /proxy/pkg/build.sh ${VERSION} ${TS}
	
push-packages:
	docker run -v $(shell pwd)/../:/proxy builder /proxy/pkg/upload_to_packagecloud.sh ${REPO} /proxy/pkg/package_cloud.conf /proxy/pkg

clean:
	cd ../ && mvn clean
	rm wavefront-proxy.jar
