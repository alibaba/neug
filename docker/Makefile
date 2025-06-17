MKFILE_PATH 			     := $(abspath $(lastword $(MAKEFILE_LIST)))
WORKING_DIR 			     := $(dir $(MKFILE_PATH))
DOCKERFILES_DIR			   	 := $(WORKING_DIR)

ifeq ($(REGISTRY),)
    REGISTRY := registry.cn-hongkong.aliyuncs.com
endif

PLATFORM          := $(shell uname -m)
# can be: x86_64, arm64
ARCH              := $(shell uname -m)


VERSION ?= latest
PROFILE ?= release

BUILD_PROGRESS  	?= auto

.PHONY: all graphscope

# High order modules
.PHONY: neug

# Base images and develop images
.PHONY: neug-manylinux

all: neug neug-manylinux
graphscope: all

neug-manylinux:
	cd .. && \
	docker build \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg PLATFORM=${PLATFORM} \
		--build-arg ARCH=${ARCH} \
		-t graphscope/neug-manylinux:${ARCH}  \
		-f ${DOCKERFILES_DIR}/manylinux2014.Dockerfile .
