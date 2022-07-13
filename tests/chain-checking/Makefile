UUID_E := $(shell uuidgen)
UUID_C := $(shell uuidgen)

all: test-chain-checking

.check-env:
ifndef WF_URL
	$(error WF_URL is undefined)
endif
ifndef WF_TOKEN
	$(error WF_TOKEN is undefined)
endif

test-chain-checking: .check-env
	UUID_E=${UUID_E} UUID_C=${UUID_C} WF_URL=${WF_URL} WF_TOKEN=${WF_TOKEN} docker compose up --build -d --remove-orphans
	sleep 30
	docker compose kill
	docker compose logs 
	docker compose rm -f -v

	curl -f -H 'Authorization: Bearer ${WF_TOKEN}' \
			-H 'Content-Type: application/json' \
			"https://${WF_URL}/api/v2/proxy/${UUID_E}" 

	curl -f -H 'Authorization: Bearer ${WF_TOKEN}' \
			-H 'Content-Type: application/json' \
			"https://${WF_URL}/api/v2/proxy/${UUID_C}" 



test-chain-run: .check-env
	UUID_E=${UUID_E} UUID_C=${UUID_C} WF_URL=${WF_URL} WF_TOKEN=${WF_TOKEN} docker compose up --build --remove-orphans
