TS = $(shell date +%Y%m%d%H%M%S)
VERSION = $(shell mvn -f ../proxy -q -Dexec.executable=echo -Dexec.args='$${project.version}' --non-recursive exec:exec)

all: build-jar prepare-builder build-packages


build-jar:
	cd ../ && mvn -f proxy --batch-mode package
	cp ../proxy/target/proxy-*-uber.jar wavefront-proxy.jar

prepare-builder:
	docker build -t builder .

build-packages:
	docker run -v $(shell pwd)/../:/proxy builder /proxy/pkg/build.sh ${VERSION} ${TS}
	
clean:
	cd ../ && mvn clean
	rm wavefront-proxy.jar
