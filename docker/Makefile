MKFILE_PATH 			     := $(abspath $(lastword $(MAKEFILE_LIST)))
WORKING_DIR 			     := $(dir $(MKFILE_PATH))
DOCKERFILES_DIR			   	 := $(WORKING_DIR)

ifeq ($(REGISTRY),)
    REGISTRY := registry.cn-hongkong.aliyuncs.com
endif

PLATFORM          := $(shell uname -m)
# can be: x86_64, arm64
ARCH              := $(shell uname -m)
# if arch is x86_64, then manylinux version is 2014, if arm64, then manylinux version is 2_28
MANYLINUXVERSION := $(if $(filter $(ARCH),x86_64),2014,2_28)


VERSION ?= latest
PROFILE ?= release

BUILD_PROGRESS  	?= auto

# High order modules
.PHONY: neug-dev 

.PHONY: neug-release

# Base images and develop images
.PHONY: neug-manylinux

all: neug-dev neug-manylinux neug-release

neug-manylinux:
	cd .. && \
	docker build \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg PLATFORM=${PLATFORM} \
		--build-arg ARCH=${ARCH} \
		--build-arg MANYLINUXVERSION=${MANYLINUXVERSION} \
		-t graphscope/neug-manylinux:${ARCH}  \
		-f ${DOCKERFILES_DIR}/neug-manylinux.Dockerfile .

neug-dev:
	cd .. && \
	docker build \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg ARCH=${ARCH} \
		-t graphscope/neug-dev:${ARCH}  \
		-f ${DOCKERFILES_DIR}/neug-dev.Dockerfile .

neug-release:
	cd .. && \
	docker build \
	-t graphscope/neug:${ARCH} \
	-f docker/neug-release.Dockerfile .