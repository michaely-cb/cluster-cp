
.PHONY: build
build:
	mkdir -p amp_stream_test
	cp run_amp_stream_test.sh load_elf.sh start_br.sh test_amp_stream_wrapper.sh 40-WIOS-PG1-bench.elf \
		40-WIOS-PG4-bench.elf 40-WIOS-PG1-wio_flows.json 40-WIOS-PG4-wio_flows.json amp-stream.template.yaml \
		${GITTOP}/tests/ws/common/ws_servers.py \
		amp_stream_test
	tar cfz amp_stream_test.tar.gz amp_stream_test

.PHONY: clean
clean:
	rm -rf amp_stream_test
