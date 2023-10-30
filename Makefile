.PHONY: build schedule


CURRENT_DIR := $(shell pwd)

all:build

build:
	-rm bgp_elastic_uploader
	@go build -o bgp_elastic_uploader

schedule:
	@(crontab -l ; echo "22 5 * * 0 cd $(CURRENT_DIR); ./bgp_elastic_uploader") | crontab -