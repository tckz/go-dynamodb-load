.PHONY: dist test clean all

export GO111MODULE=on

ifeq ($(GO_CMD),)
GO_CMD:=go
endif

VERSION := $(shell git describe --always)
GO_BUILD := $(GO_CMD) build -ldflags "-X main.version=$(VERSION)"

DIR_DIST = dist

DISTS = \
	$(DIR_DIST)/dynamodb-load \
	$(DIR_DIST)/dynamodb-load-same-partition-batch \
	$(DIR_DIST)/gen

TARGETS = $(DISTS)

SRCS_OTHER := $(shell find . \
	-type d -name cmd -prune -o \
	-type f -name "*.go" -print) go.mod

all: $(TARGETS)
	@echo "$@ done." 1>&2

clean:
	/bin/rm -f $(TARGETS)
	@echo "$@ done." 1>&2

$(DIR_DIST)/dynamodb-load-same-partition-batch: cmd/dynamodb-load-same-partition-batch/* $(SRCS_OTHER)
	$(GO_BUILD) -o $@ ./cmd/dynamodb-load-same-partition-batch/

$(DIR_DIST)/dynamodb-load: cmd/dynamodb-load/* $(SRCS_OTHER)
	$(GO_BUILD) -o $@ ./cmd/dynamodb-load/

$(DIR_DIST)/gen: cmd/gen/* $(SRCS_OTHER)
	$(GO_BUILD) -o $@ ./cmd/gen/


