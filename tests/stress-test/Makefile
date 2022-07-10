.check-env:
ifndef WF_URL
	$(error WF_URL is undefined)
endif
ifndef WF_TOKEN
	$(error WF_TOKEN is undefined)
endif

.setup: .check-env
	cd resources/metrics_filter && npm install
	cp ../../docker/wavefront-proxy.jar docker/proxy/

stress-local-loadgen: .setup
	WF_URL=${WF_URL} WF_TOKEN=${WF_TOKEN} docker compose --profile loadgen up --build --remove-orphans --force-recreate

stress-local-jmeter: .setup
	WF_URL=${WF_URL} WF_TOKEN=${WF_TOKEN} docker compose --profile jmeter up --build --remove-orphans --force-recreate


