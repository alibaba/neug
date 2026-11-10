# NeuG Images Overview

## Image Retrieval from Registry

To access NeuG images, you can pull them from the specified registry using the commands below:

```bash
# Pulling images for different architectures
docker pull neug-registry.cn-hongkong.cr.aliyuncs.com/neug/neug-manylinux:2026.03.13-2-x86_64
docker pull neug-registry.cn-hongkong.cr.aliyuncs.com/neug/neug-manylinux:2026.03.13-2-arm64

# Development image (a single multi-arch manifest list; Docker picks the right arch on pull)
docker pull neug-registry.cn-hongkong.cr.aliyuncs.com/neug/neug-dev:2026.03.13-2
```

## Image Tag Convention

All build-environment images share a single **generation tag** (e.g. `2026.03.13-2`). The tag is decoupled from the NeuG release version and is bumped only when a build input actually changes (`scripts/install_deps.sh`, any of the three Dockerfiles, or the upstream PyPA `BASETAG`).

When the generation is bumped, **all five images are rebuilt or retagged together** — even if a particular image's own content is unchanged (e.g. `neug-dev` when only `BASETAG` moves). This keeps the invariant: one tag greps the entire repo and uniquely identifies the build-environment generation. For `neug-dev` (multi-arch manifest list), a retag is a zero-cost server-side `docker buildx imagetools create`.

## Building Manylinux Images

To create wheel packages compatible with various Linux distributions, we utilize the manylinux approach. Use the following commands to build images equipped with the necessary environments for NeuG:

```bash
make neug-manylinux
```

And tag the images to the desired registry

```bash
export ARCH=arm64 # x86_64
docker tag neug/neug-manylinux:${ARCH} neug-registry.cn-hongkong.cr.aliyuncs.com/neug/neug-manylinux:2026.03.13-2-${ARCH}
```

## Developing Development Images

For development purposes, build the development image using:

```bash
make neug-dev   # Build the image suitable for development
```

## Creating Manifests

The `neug-manylinux` images are architecture-specific, so you can create a manifest to enable users to pull the same image name across different platforms. This process is scripted in `manifest.sh`.

```bash
bash ./manifest.sh neug-manylinux 2026.03.13-2 hongkong create
bash ./manifest.sh neug v0.3.0 hongkong create
```

`neug-dev` is different: it is published as a single **multi-arch manifest list** with **no per-arch tags**, so `manifest.sh` (which combines `<tag>-x86_64` + `<tag>-arm64`) does not apply, and `pull`/`tag`/`push` would flatten it to one architecture and break arm64 runners. Copy the whole list server-side with `docker buildx imagetools create` instead. Note that `manifest.sh` targets the shared ACR endpoint (`registry.cn-hongkong.aliyuncs.com/neug`), whereas CI pulls from the enterprise endpoint (`neug-registry.cn-hongkong.cr.aliyuncs.com/neug`), so run `imagetools` against the enterprise endpoint:

```bash
REG=neug-registry.cn-hongkong.cr.aliyuncs.com/neug
docker buildx imagetools create -t $REG/neug-dev:2026.03.13-2 $REG/neug-dev:v0.3.0
docker buildx imagetools inspect  $REG/neug-dev:2026.03.13-2   # should list linux/amd64 + linux/arm64
```

Ensure images are pushed to the registry before executing the manifest creation.

## Package Build Compatibility

The `neug-release` image builds and installs a Python wheel. That wheel is a
portable package artifact, so the release image build passes
`NEUG_PACKAGE_BUILD=ON` and `NEUG_NATIVE_ARCH=OFF`. Development and manylinux
images are build environments and do not force package mode by themselves.

## manylinux_2_28 for node

To support Node.js 20 and later, use the newer manylinux_2_28 images for Node.js
package builds:

```bash
docker pull neug-registry.cn-hongkong.cr.aliyuncs.com/neug/neug-manylinux:2026.03.13-2-x86_64-node
docker pull neug-registry.cn-hongkong.cr.aliyuncs.com/neug/neug-manylinux:2026.03.13-2-arm64-node
```

Build the image locally with:

```bash
make neug-manylinux-node
```

And tag the image before pushing it to the registry:

```bash
export ARCH=arm64 # x86_64
docker tag neug/neug-manylinux:${ARCH}-node neug-registry.cn-hongkong.cr.aliyuncs.com/neug/neug-manylinux:2026.03.13-2-${ARCH}-node
```
