all: testDD

.check-env:
ifndef WF_SERVER
	$(error WF_SERVER is undefined)
endif
ifndef WF_TOKEN
	$(error WF_TOKEN is undefined)
endif
ifndef DD_API_KEY
	$(error DD_API_KEY is undefined)
endif

testDD: .check-env
	WF_SERVER=${WF_SERVER} WF_TOKEN=${WF_TOKEN} DD_API_KEY=${DD_API_KEY} docker compose up --build --attach wf-proxy
