.PHONY: build schedule


CURRENT_DIR := $(shell pwd)

LOGROTATE_CONF := /etc/logrotate.d/bgp-elastic-uploader

LOG_PATH := /var/log/bgp-elastic-uploader

define LOGROTATE_CONF_CONTENT
$(LOG_PATH)/*.log {
    rotate 5
    weekly
    missingok
    notifempty
    compress
    create 644 root root
}
endef

export LOGROTATE_CONF_CONTENT

all:build

init:
	-mkdir $(LOG_PATH)
	@echo "$$LOGROTATE_CONF_CONTENT" > $(LOGROTATE_CONF)

build:init
	-rm bgp-elastic-uploader
	@go build -o bgp-elastic-uploader

schedule:
	@(crontab -l ; echo "22 5 * * 0 cd $(CURRENT_DIR); ./bgp-elastic-uploader") | crontab -