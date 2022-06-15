.check-env:
ifndef WF_URL
	$(error WF_URL is undefined)
endif
ifndef WF_TOKEN
	$(error WF_TOKEN is undefined)
endif

stress-local: .check-env
	cd resources/metrics_filter && npm install
	cp ../../docker/wavefront-proxy.jar docker/proxy/
	WF_URL=${WF_URL} WF_TOKEN=${WF_TOKEN} docker compose up --build --remove-orphans --force-recreate


