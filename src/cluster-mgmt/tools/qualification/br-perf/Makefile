build:
	mkdir -p br-perf
	docker build . --platform linux/amd64 -t registry.local/alpine-iperf:0.0.1
	docker save registry.local/alpine-iperf:0.0.1 -o br-perf/image.tar
	cp run.sh br-perf
	tar -cf br-perf.tar br-perf
	rm -rf br-perf

clean:
	rm -rf br-perf
	rm -f br-perf.tar

.PHONY: build clean