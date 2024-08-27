# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License.

GO		  ?= go
PWD 	  := $(shell pwd)
GOPATH	:= $(shell $(GO) env GOPATH)
SHELL 	:= /bin/bash

INSTALL_PATH := $(PWD)/bin
LIBRARY_PATH := $(PWD)/lib
PGO_PATH := $(PWD)/configs/pgo
OS := $(shell uname -s)
mode = Release

ifeq (${ENABLE_AZURE}, false)
	AZURE_OPTION := -Z
endif

storage-test: print-build-info
	@echo "Building storage-test ..."
	@mkdir -p $(INSTALL_PATH) && go env -w CGO_ENABLED="0" && \
		GO111MODULE=on $(GO) build -pgo=$(PGO_PATH)/default.pgo \
		-ldflags="-extldflags=-static" -o $(INSTALL_PATH)/storage-test $(PWD)/cmd/main.go 1>/dev/null


print-build-info:
	$(shell git config --global --add safe.directory '*')
	@echo "Build Tag: $(BUILD_TAGS)"
	@echo "Build Time: $(BUILD_TIME)"
	@echo "Git Commit: $(GIT_COMMIT)"
	@echo "Go Version: $(GO_VERSION)"

fmt:
ifdef GO_DIFF_FILES
	@echo "Running $@ check"
	@GO111MODULE=on env bash $(PWD)/scripts/gofmt.sh $(GO_DIFF_FILES)
else
	@echo "Running $@ check"
	@GO111MODULE=on env bash $(PWD)/scripts/gofmt.sh cmd/
	@GO111MODULE=on env bash $(PWD)/scripts/gofmt.sh internal/
	@GO111MODULE=on env bash $(PWD)/scripts/gofmt.sh pkg/
endif

# Build each component and install binary to $GOPATH/bin.
install: storage-test
	@echo "Installing binary to './bin'"
	@mkdir -p $(GOPATH)/bin && cp -f $(PWD)/bin/storage-test $(GOPATH)/bin/storage-test
	@echo "Installation successful."

