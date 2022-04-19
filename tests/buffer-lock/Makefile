tmp_dir := $(shell mktemp -d -t ci-XXXXXXXXXX)

all: test-buffer-lock

.check-env:
ifndef WF_URL
	$(error WF_URL is undefined)
endif
ifndef WF_TOKEN
	$(error WF_TOKEN is undefined)
endif

test-buffer-lock: .check-env
	@[ -d ${tmp_dir} ] || exit -1
	WF_URL=${WF_URL} WF_TOKEN=${WF_TOKEN} docker-compose up --build -d
	sleep 10
	docker-compose kill
	docker-compose logs --no-color | tee ${tmp_dir}/out.txt
	docker-compose rm -f -v
	echo ${tmp_dir}

	grep OverlappingFileLockException $(tmp_dir)/out.txt || $(MAKE) .error
	$(MAKE) .clean

.clean:
	@rm -rf ${tmp_dir}

.error: .clean
	@echo
	@echo ERROR !!
	@exit 1

.ok: .clean
	@echo
	@echo OK !!
	@exit 0
