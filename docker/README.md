# NeuG Images Overview

## Image Retrieval from Registry

To access NeuG images, you can pull them from the specified registry using the commands below:

```bash
# Pulling images for different architectures
docker pull neug-registry.cn-hongkong.cr.aliyuncs.com/neug/neug-manylinux:v0.1.0-x86_64
docker pull neug-registry.cn-hongkong.cr.aliyuncs.com/neug/neug-manylinux:v0.1.0-arm64

# Development images
docker pull neug-registry.cn-hongkong.cr.aliyuncs.com/neug/neug-dev:v0.1.0-x86_64
docker pull neug-registry.cn-hongkong.cr.aliyuncs.com/neug/neug-dev:v0.1.0-arm64
```

## Building Manylinux Images

To create wheel packages compatible with various Linux distributions, we utilize the manylinux approach. Use the following commands to build images equipped with the necessary environments for NeuG:

```bash
make neug-manylinux
```

And tag the images to the desired registry

```bash
export ARCH=aarch64 # x86_64
docker tag graphscope/neug-manylinux:${ARCH} neug-registry.cn-hongkong.cr.aliyuncs.com/neug/neug-manylinux:v0.1.0-${ARCH}
```

## Developing Development Images

For development purposes, build the development image using:

```bash
make neug-dev   # Build the image suitable for development
```

## Creating Manifests

Since Docker images are architecture-specific, you can create a manifest to enable users to pull the same image name across different platforms. This process is scripted in `manifest.sh`.

```bash
bash ./manifest.sh neug-manylinux v0.1.0 hongkong create
bash ./manifest.sh neug v0.1.0 hongkong create
bash ./manifest.sh neug-dev v0.1.0 hongkong create
```

Ensure images are pushed to the registry before executing the manifest creation.