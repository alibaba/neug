#!/bin/bash
# Copyright 2020 Alibaba Group Holding Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

# blue
info() {
  printf "\e[34m%b\e[0m\n" "$*"
}

# red
err() {
  printf "\e[31m%b\e[0m\n" "$*"
}

# yellow
warning() {
  printf "\e[1;33m%b\e[0m\n" "$*"
}

# red
debug() {
  printf "\e[31m%b\e[0m\n" "[DEBUG] $*"
}

get_os_version() {
  if [ -f /etc/centos-release ]; then
    # Older Red Hat, CentOS, Alibaba Cloud Linux etc.
    PLATFORM=CentOS
    OS_VERSION=$(sed 's/.* \([0-9]\).*/\1/' < /etc/centos-release)
    if grep -q "Alibaba Cloud Linux" /etc/centos-release; then
      PLATFORM="Aliyun_based_on_CentOS"
      OS_VERSION=$(rpm -E %{rhel})
    fi
  elif [ -f /etc/os-release ]; then
    # freedesktop.org and systemd
    . /etc/os-release
    PLATFORM="${NAME}"
    OS_VERSION="${VERSION_ID}"
  elif type lsb_release >/dev/null 2>&1; then
    # linuxbase.org
    PLATFORM=$(lsb_release -si)
    OS_VERSION=$(lsb_release -sr)
  elif [ -f /etc/lsb-release ]; then
    # For some versions of Debian/Ubuntu without lsb_release command
    . /etc/lsb-release
    PLATFORM="${DISTRIB_ID}"
    OS_VERSION="${DISTRIB_RELEASE}"
  elif [ -f /etc/debian_version ]; then
    # Older Debian/Ubuntu/etc.
    PLATFORM=Debian
    OS_VERSION=$(cat /etc/debian_version)
  else
    # Fall back to uname, e.g. "Linux <version>", also works for BSD, Darwin, etc.
    PLATFORM=$(uname -s)
    OS_VERSION=$(uname -r)
  fi
  if [[ "${PLATFORM}" != *"Ubuntu"* && "${PLATFORM}" != *"CentOS"* && "${PLATFORM}" != *"Darwin"* && "${PLATFORM}" != *"Aliyun"* ]];then
    err "Only support on Ubuntu/CentOS/macOS/AliyunOS platform."
    exit 1
  fi
  if [[ "${PLATFORM}" == *"Ubuntu"* && "${OS_VERSION:0:2}" -lt "20" ]]; then
    err "Ubuntu ${OS_VERSION} found, requires 20 or greater."
    exit 1
  fi
  if [[ "${PLATFORM}" == *"CentOS"* && "${OS_VERSION}" -lt "7" ]]; then
    err "CentOS ${OS_VERSION} found, requires 8 or greater."
    exit 1
  fi
  if [[ "${PLATFORM}" == *"Darwin"* ]]; then
    export HOMEBREW_NO_INSTALL_CLEANUP=1
    export HOMEBREW_NO_INSTALLED_DEPENDENTS_CHECK=1
  fi
  echo "$PLATFORM-$OS_VERSION"
}

# default values
readonly OS=$(get_os_version)
readonly OS_PLATFORM=${OS%-*}
readonly OS_VERSION=${OS#*-}
readonly ARCH=$(uname -m)
readonly OUTPUT_ENV_FILE="${HOME}/.graphscope_env"
if [[ "${OS_PLATFORM}" == *"Darwin"* ]]; then
  readonly HOMEBREW_PREFIX=$(brew --prefix)
fi
readonly ARROW_VERSION="15.0.2"
readonly tempdir="/tmp/gs-local-deps"
cn_flag=false
debug_flag=false
install_prefix="/opt/graphscope"

# parse args
while (( "$#" )); do
  case "$1" in
    --install-prefix)
      install_prefix="$2"
      shift 2
      ;;
    --cn)
      cn_flag=true
      shift
      ;;
    --debug)
      debug_flag=true
      shift
      ;;
    *)
      shift
      ;;
  esac
done


if [[ ${debug_flag} == true ]]; then
  debug "OS: ${OS}, OS_PLATFORM: ${OS_PLATFORM}, OS_VERSION: ${OS_VERSION}"
  debug "install dependencies for NexG, instanll prefix ${install_prefix}"
fi

# sudo
SUDO=sudo
if [[ $(id -u) -eq 0 ]]; then
  SUDO=""
fi

# speed up
if [ "${cn_flag}" == true ]; then
  export HOMEBREW_BREW_GIT_REMOTE="https://mirrors.tuna.tsinghua.edu.cn/git/homebrew/brew.git"
  export HOMEBREW_CORE_GIT_REMOTE="https://mirrors.tuna.tsinghua.edu.cn/git/homebrew/homebrew-core.git"
  export HOMEBREW_BOTTLE_DOMAIN="https://mirrors.tuna.tsinghua.edu.cn/homebrew-bottles"
fi

# install functions
init_workspace_and_env() {
  info "creating directory: ${install_prefix} ${tempdir}"
  ${SUDO} mkdir -p ${install_prefix} ${tempdir}
  ${SUDO} chown -R $(id -u):$(id -g) ${install_prefix} ${tempdir}
  export PATH=${install_prefix}/bin:${PATH}
  export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${install_prefix}/lib:${install_prefix}/lib64
}

# utils functions
function set_to_cn_url() {
  local url=$1
  if [[ ${cn_flag} == true ]]; then
    url="https://graphscope.oss-cn-beijing.aliyuncs.com/dependencies"
  fi
  echo ${url}
}

function fetch_source() {
  local url=$1
  local file=$2
  info "Downloading ${file} from ${url}"
  curl -fsSL -o "${file}" "${url}/${file}"
}

function download_and_untar() {
  local url=$1
  local file=$2
  local directory=$3
  if [ ! -d "${directory}" ]; then
    [ ! -f "${file}" ] && fetch_source "${url}" "${file}"
    tar zxf "${file}"
  fi
}

function git_clone() {
  local url=$1
  local file=$2
  local directory=$3
  local branch=$4
  if [ ! -d "${directory}" ]; then
    if [ ! -f "${file}" ]; then
      git clone --depth=1 --branch "${branch}" "${url}" "${directory}"
      pushd "${directory}" || exit
      git submodule update --init || true
      popd || exit
    else
      tar zxf "${file}"
    fi
  fi
}


# boost with leaf for centos and ubuntu
install_boost() {
  if [[ -f "${install_prefix}/include/boost/version.hpp" ]]; then
    return 0
  fi
  pushd "${tempdir}" || exit
  directory="boost_1_75_0"
  file="${directory}.tar.gz"
  url="https://archives.boost.io/release/1.75.0/source"
  url=$(set_to_cn_url ${url})
  download_and_untar "${url}" "${file}" "${directory}"
  pushd ${directory} || exit
  # seastar needs filesystem program_options thread unit_test_framework
  # interactive needs context regex date_time
  ./bootstrap.sh --prefix="${install_prefix}" \
    --with-libraries=system,filesystem,context,program_options,regex,thread,random,chrono,atomic,date_time,test
  ./b2 install link=shared runtime-link=shared variant=release threading=multi
  popd || exit
  popd || exit
  rm -rf "${tempdir:?}/${directory:?}" "${tempdir:?}/${file:?}"
}

# arrow for ubuntu and centos
install_arrow() {
  if [[ "${OS_PLATFORM}" == *"Ubuntu"* ]]; then
    if ! dpkg -s libarrow-dev &>/dev/null; then
      ${SUDO} apt-get install -y lsb-release
      # shellcheck disable=SC2046,SC2019,SC2018
      wget -c https://apache.jfrog.io/artifactory/arrow/"$(lsb_release --id --short | tr 'A-Z' 'a-z')"/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb -P /tmp/
      ${SUDO} apt-get install -y -V /tmp/apache-arrow-apt-source-latest-"$(lsb_release --codename --short)".deb
      ${SUDO} apt-get update -y
      ${SUDO} apt-get install -y libarrow-dev=${ARROW_VERSION}-1 libarrow-dataset-dev=${ARROW_VERSION}-1 libarrow-acero-dev=${ARROW_VERSION}-1 libparquet-dev=${ARROW_VERSION}-1
      rm /tmp/apache-arrow-apt-source-latest-*.deb
    fi
  else
    install_arrow_from_source
  fi
}

# arrow for centos
install_arrow_from_source() {
  if [[ -f "${install_prefix}/include/arrow/api.h" ]]; then
    return 0
  fi
  pushd "${tempdir}" || exit
  directory="arrow-apache-arrow-${ARROW_VERSION}"
  file="apache-arrow-${ARROW_VERSION}.tar.gz"
  url="https://github.com/apache/arrow/archive"
  url=$(set_to_cn_url ${url})
  download_and_untar "${url}" "${file}" "${directory}"
  pushd ${directory} || exit
  cmake ./cpp \
    -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
    -DCMAKE_PREFIX_PATH="${install_prefix}" \
    -DCMAKE_INSTALL_PREFIX="${install_prefix}" \
    -DARROW_COMPUTE=ON \
    -DARROW_WITH_UTF8PROC=OFF \
    -DARROW_CSV=ON \
    -DARROW_CUDA=OFF \
    -DARROW_DATASET=OFF \
    -DARROW_FILESYSTEM=ON \
    -DARROW_FLIGHT=OFF \
    -DARROW_GANDIVA=OFF \
    -DARROW_HDFS=OFF \
    -DARROW_JSON=OFF \
    -DARROW_ORC=OFF \
    -DARROW_PARQUET=OFF \
    -DARROW_PLASMA=OFF \
    -DARROW_PYTHON=OFF \
    -DARROW_S3=OFF \
    -DARROW_WITH_BZ2=OFF \
    -DARROW_WITH_ZLIB=OFF \
    -DARROW_WITH_LZ4=OFF \
    -DARROW_WITH_SNAPPY=OFF \
    -DARROW_WITH_ZSTD=OFF \
    -DARROW_WITH_BROTLI=OFF \
    -DARROW_IPC=ON \
    -DARROW_BUILD_BENCHMARKS=OFF \
    -DARROW_BUILD_EXAMPLES=OFF \
    -DARROW_BUILD_INTEGRATION=OFF \
    -DARROW_BUILD_UTILITIES=OFF \
    -DARROW_BUILD_TESTS=OFF \
    -DARROW_ENABLE_TIMING_TESTS=OFF \
    -DARROW_FUZZING=OFF \
    -DARROW_USE_ASAN=OFF \
    -DARROW_USE_TSAN=OFF \
    -DARROW_USE_UBSAN=OFF \
    -DARROW_JEMALLOC=OFF \
    -DARROW_BUILD_SHARED=ON \
    -DARROW_BUILD_STATIC=OFF
  make -j$(nproc)
  make install
  popd || exit
  popd || exit
  rm -rf "${tempdir:?}/${directory:?}" "${tempdir:?}/${file:?}"
}

install_mimalloc() {
  pushd "${tempdir}" || exit
  git clone https://github.com/microsoft/mimalloc -b v1.8.6
  cd mimalloc
  mkdir -p build && cd build
  cmake .. -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="${install_prefix}"
  make -j$(nproc)
  make install
  popd || exit
  rm -rf "${tempdir:?}/mimalloc"
}

install_yaml_cpp() {
  pushd "${tempdir}" || exit
  git clone https://github.com/jbeder/yaml-cpp.git -b 0.8.0
  cd yaml-cpp
  mkdir -p build && cd build
  cmake .. -DYAML_BUILD_SHARED_LIBS=ON  -DCMAKE_INSTALL_PREFIX="${install_prefix}" -DCMAKE_POLICY_VERSION_MINIMUM=3.5
  make -j$(nproc)
  make install
  popd || exit
  rm -rf "${tempdir:?}/yaml-cpp"
}

install_protobuf() {
  if [[ -f "${install_prefix}/include/google/protobuf/port.h" ]]; then
    return 0
  fi
  pushd "${tempdir}" || exit
  directory="protobuf-21.9"
  file="protobuf-all-21.9.tar.gz"
  url="https://github.com/protocolbuffers/protobuf/releases/download/v21.9"
  url=$(set_to_cn_url ${url})
  download_and_untar "${url}" "${file}" "${directory}"
  pushd ${directory} || exit
  ./configure --prefix="${install_prefix}" --enable-shared --disable-static
  make -j$(nproc)
  make install
  popd || exit
  popd || exit
  rm -rf "${tempdir:?}/${directory:?}" "${tempdir:?}/${file:?}"
}

BASIC_PACKAGES_LINUX=("file" "curl" "wget" "git" "sudo")
BASIC_PACKAGES_UBUNTU=("${BASIC_PACKAGES_LINUX[@]}" "build-essential" "cmake" "libunwind-dev" "python3-pip")
BASIC_PACKAGES_CENTOS_8=("wget" "${BASIC_PACKAGES_LINUX[@]}" "epel-release" "libunwind-devel" "libcurl-devel" "perl" "which")
BASIC_PACKAGES_CENTOS_7=("${BASIC_PACKAGES_CENTOS_8[@]}" "centos-release-scl-rh" "java-11-openjdk" "java-11-openjdk-devel")
ADDITIONAL_PACKAGES_CENTOS_8=("gcc-c++" "python38-devel")
ADDITIONAL_PACKAGES_CENTOS_7=("make" "devtoolset-8-gcc-c++" "rh-python38-python-pip" "rh-python38-python-devel")

install_basic_packages() {
  if [[ "${OS_PLATFORM}" == *"Ubuntu"* ]]; then
    ${SUDO} apt-get update -y
    DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC ${SUDO} apt-get install -y ${BASIC_PACKAGES_UBUNTU[*]}
  elif [[ "${OS_PLATFORM}" == *"CentOS"* || "${OS_PLATFORM}" == *"Aliyun"* ]]; then
    ${SUDO} yum update -y
    if [[ "${OS_VERSION}" -eq "7" ]]; then
      # centos7
      ${SUDO} yum install -y ${BASIC_PACKAGES_CENTOS_7[*]}
      # change the source for centos-release-scl-rh
      ${SUDO} sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*scl*
      ${SUDO} sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*scl*
      ${SUDO} sed -i 's|# baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*scl*
      ${SUDO} yum install -y ${ADDITIONAL_PACKAGES_CENTOS_7[*]}
	  else
      if [[ "${OS_PLATFORM}" == *"Aliyun"* ]]; then
        ${SUDO} yum install -y 'dnf-command(config-manager)'
        ${SUDO} dnf install -y epel-release --allowerasing
      else
        ${SUDO} sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*
        ${SUDO} sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*
        ${SUDO} yum install -y 'dnf-command(config-manager)'
        ${SUDO} dnf install -y epel-release
        ${SUDO} dnf config-manager --set-enabled powertools
      fi
      ${SUDO} dnf config-manager --set-enabled epel
      ${SUDO} yum install -y ${BASIC_PACKAGES_CENTOS_8[*]}
      ${SUDO} yum install -y ${ADDITIONAL_PACKAGES_CENTOS_8[*]}
    fi
  fi
}

install_gflags() {
  if [[ -f "${install_prefix}/include/gflags/gflags.h" ]]; then
    return 0
  fi
  pushd "${tempdir}" || exit
  directory="gflags-2.2.2"
  file="v2.2.2.tar.gz"
  url="https://github.com/gflags/gflags/archive"
  url=$(set_to_cn_url ${url})
  download_and_untar "${url}" "${file}" "${directory}"
  pushd ${directory} || exit
  cmake . -DCMAKE_INSTALL_PREFIX="${install_prefix}" \
          -DCMAKE_PREFIX_PATH="${install_prefix}" \
          -DBUILD_SHARED_LIBS=ON
  make -j$(nproc)
  make install
  popd || exit
  popd || exit
  rm -rf "${tempdir:?}/${directory:?}" "${tempdir:?}/${file:?}"
}

install_glog() {
  if [[ -f "${install_prefix}/include/glog/logging.h" ]]; then
    return 0
  fi
  pushd "${tempdir}" || exit
  directory="glog-0.6.0"
  file="v0.6.0.tar.gz"
  url="https://github.com/google/glog/archive"
  url=$(set_to_cn_url ${url})
  download_and_untar "${url}" "${file}" "${directory}"
  pushd ${directory} || exit
  cmake . -DCMAKE_INSTALL_PREFIX="${install_prefix}" \
          -DCMAKE_PREFIX_PATH="${install_prefix}" \
          -DBUILD_SHARED_LIBS=ON
  make -j$(nproc)
  make install
  popd || exit
  popd || exit
  rm -rf "${tempdir:?}/${directory:?}" "${tempdir:?}/${file:?}"
}

install_abseil() {
  if [[ -f "${install_prefix}/include/absl/base/config.h" ]]; then
    return 0
  fi
  pushd "${tempdir}" || exit
  directory="abseil-cpp-20240722.1"
  file="abseil-cpp-20240722.1.tar.gz"
  url="https://github.com/abseil/abseil-cpp/releases/download/20240722.1"
  url=$(set_to_cn_url ${url})
  download_and_untar "${url}" "${file}" "${directory}"
  pushd ${directory} || exit
  mkdir build && pushd build && cmake ..  -DCMAKE_INSTALL_PREFIX="${install_prefix}" \
       -DCMAKE_PREFIX_PATH="${install_prefix}" -DCMAKE_CXX_STANDARD=17 -DBUILD_SHARED_LIBS=ON 
  make -j ${nproc}
  make install
  popd || exit
  popd || exit
  popd || exit
  rm -rf "${tempdir:?}/${directory:?}" "${tempdir:?}/${file:?}"
}

INTERACTIVE_MACOS=("rapidjson" "xsimd")
INTERACTIVE_UBUNTU=("rapidjson-dev" "libgoogle-glog-dev" "libgflags-dev" "libyaml-cpp-dev" "libprotobuf-dev" "libgflags-dev")
INTERACTIVE_CENTOS=("rapidjson-devel" "glog-devel")

install_nexg_dependencies() {
  # dependencies package
  if [[ "${OS_PLATFORM}" == *"Darwin"* ]]; then
    brew install ${INTERACTIVE_MACOS[*]}
    install_abseil
    install_gflags
    install_glog
    install_arrow
    install_boost
    install_yaml_cpp
    install_protobuf
    install_mimalloc
  elif [[ "${OS_PLATFORM}" == *"Ubuntu"* ]]; then
    DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC ${SUDO} apt-get install -y ${INTERACTIVE_UBUNTU[*]}
    install_arrow
    install_boost
    # hiactor is only supported on ubuntu
    install_mimalloc
    ${SUDO} sh -c 'echo "fs.aio-max-nr = 1048576" >> /etc/sysctl.conf'
    ${SUDO} sysctl -p /etc/sysctl.conf
  else
    ${SUDO} yum install -y ${INTERACTIVE_CENTOS[*]}
    install_arrow
    install_boost
    install_mimalloc
    install_yaml_cpp
    install_protobuf
    install_gflags
  fi
}

write_env_config() {
  echo "" > ${OUTPUT_ENV_FILE}
  # common
  {
    echo "export GRAPHSCOPE_HOME=${install_prefix}"
    echo "export CMAKE_PREFIX_PATH=/opt/vineyard:/opt/graphscope/"
    echo "export PATH=${install_prefix}/bin:\$HOME/.local/bin:\$HOME/.cargo/bin:\$PATH"
    echo "export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}"
    echo "export LIBRARY_PATH=${install_prefix}/lib:${install_prefix}/lib64"
  } >> "${OUTPUT_ENV_FILE}"
  {
    if [[ "${OS_PLATFORM}" == *"CentOS"* || "${OS_PLATFORM}" == *"Aliyun"* ]]; then
      if [[ "${OS_VERSION}" -eq "7" ]]; then
        echo "source /opt/rh/devtoolset-8/enable"
        echo "source /opt/rh/rh-python38/enable"
      fi
    fi
  } >> "${OUTPUT_ENV_FILE}"
}

install_deps() {
  init_workspace_and_env
  install_basic_packages
  install_nexg_dependencies
  write_env_config
  info "The script has installed all dependencies, don't forget to exec command:\n
  $ source ${OUTPUT_ENV_FILE}
  \nbefore building NexG."
}

install_deps
