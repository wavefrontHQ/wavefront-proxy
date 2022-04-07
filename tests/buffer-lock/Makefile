tmp_dir := $(shell mktemp -d -t ci-XXXXXXXXXX)

all: test-buffer-lock

test-buffer-lock:
	@[ -d ${tmp_dir} ] || exit -1
	docker-compose up --build -d
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
