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
readonly OUTPUT_ENV_FILE="${HOME}/.neug_env"
if [[ "${OS_PLATFORM}" == *"Darwin"* ]]; then
  readonly HOMEBREW_PREFIX=$(brew --prefix)
fi
readonly tempdir="/tmp/gs-local-deps"
cn_flag=false
debug_flag=false
install_prefix="/opt/neug"
ARROW_VERSION=${ARROW_VERSION:-"18.0.0"}

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
  debug "install dependencies for NeuG, instanll prefix ${install_prefix}"
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
  # if LD_LIBRARY_PATH is not set
  if [[ -z "${LD_LIBRARY_PATH}" ]]; then
    export LD_LIBRARY_PATH=${install_prefix}/lib:${install_prefix}/lib64
  else
    export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${install_prefix}/lib:${install_prefix}/lib
  fi
  #if macos
  if [[ "${OS_PLATFORM}" == *"Darwin"* ]]; then
    export MACOSX_DEPLOYMENT_TARGET=10.15
  fi
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

install_arrow_from_source() {
  local arrow_version="${ARROW_VERSION}"
  local arrow_archive="apache-arrow-${arrow_version}.tar.gz"
  local arrow_directory="apache-arrow-${arrow_version}"
  local arrow_build_directory="${tempdir}/apache-arrow-build"
  local arrow_url="https://graphscope.oss-cn-beijing.aliyuncs.com/apache-arrow-${arrow_version}.tar.gz"

  if [[ -f "${install_prefix}/lib/libarrow.a" || -f "${install_prefix}/lib64/libarrow.a" ]]; then
    info "Arrow already installed under ${install_prefix}, skip."
    return 0
  fi

  info "Installing Arrow ${arrow_version} from source"
  pushd "${tempdir}" >/dev/null || exit
  if [ ! -f "${arrow_archive}" ]; then
    info "Downloading Arrow archive from ${arrow_url}"
    curl -fsSL -o "${arrow_archive}" "${arrow_url}"
  fi
  if [ ! -d "${arrow_directory}" ]; then
    tar zxf "${arrow_archive}"
  fi

  cmake -S "${arrow_directory}/cpp" -B "${arrow_build_directory}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="${install_prefix}" \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DARROW_BUILD_SHARED=OFF \
    -DARROW_BUILD_STATIC=ON \
    -DARROW_WITH_UTF8PROC=OFF \
    -DARROW_CSV=ON \
    -DARROW_ACERO=ON \
    -DARROW_DATASET=ON \
    -DARROW_COMPUTE=ON \
    -DARROW_CUDA=OFF \
    -DARROW_FILESYSTEM=ON \
    -DARROW_FLIGHT=OFF \
    -DARROW_GANDIVA=OFF \
    -DARROW_HDFS=OFF \
    -DARROW_ORC=OFF \
    -DARROW_JSON=ON \
    -DARROW_PARQUET=OFF \
    -DARROW_PLASMA=OFF \
    -DARROW_PYTHON=OFF \
    -DARROW_S3=OFF \
    -DARROW_WITH_BZ2=OFF \
    -DARROW_WITH_LZ4=OFF \
    -DARROW_WITH_SNAPPY=OFF \
    -DARROW_WITH_ZSTD=OFF \
    -DARROW_WITH_BROTLI=OFF \
    -DARROW_IPC=ON \
    -DARROW_BUILD_BENCHMARKS=OFF \
    -DARROW_BUILD_TESTS=OFF \
    -DARROW_BUILD_EXAMPLES=OFF \
    -DARROW_BUILD_UTILITIES=OFF \
    -DARROW_BUILD_INTEGRATION=OFF \
    -DARROW_ENABLE_TIMING_TESTS=OFF \
    -DARROW_FUZZING=OFF \
    -DARROW_USE_ASAN=OFF \
    -DARROW_USE_UBSAN=OFF \
    -DARROW_USE_TSAN=OFF \
    -DARROW_USE_JEMALLOC=OFF \
    -DARROW_SIMD_LEVEL=NONE \
    -DARROW_RUNTIME_SIMD_LEVEL=NONE \
    -DARROW_POSITION_INDEPENDENT_CODE=ON \
    -DARROW_DEPENDENCY_SOURCE=BUNDLED \
    -DRapidJSON_SOURCE=BUNDLED

  cmake --build "${arrow_build_directory}" --target install -j"$(nproc)"
  popd >/dev/null || exit
  rm -rf "${arrow_build_directory}" "${tempdir}/${arrow_directory}" "${tempdir}/${arrow_archive}"
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
      ${SUDO} sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*scl* || echo "No CentOS scl repo found, skipping."
      ${SUDO} sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*scl* || echo "No CentOS scl repo found, skipping."
      ${SUDO} sed -i 's|# baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*scl* || echo "No CentOS scl repo found, skipping."
      ${SUDO} yum install -y ${ADDITIONAL_PACKAGES_CENTOS_7[*]}
	  else
      if [[ "${OS_PLATFORM}" == *"Aliyun"* ]]; then
        ${SUDO} yum install -y 'dnf-command(config-manager)'
        ${SUDO} dnf install -y epel-release --allowerasing
      else
        ${SUDO} sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-* || echo "No CentOS repo found, skipping."
        ${SUDO} sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-* || echo "No CentOS repo found, skipping."
        ${SUDO} yum install -y 'dnf-command(config-manager)' || echo "No dnf-command found, skipping."
        ${SUDO} dnf install -y epel-release
        ${SUDO} dnf config-manager --set-enabled powertools
      fi
      ${SUDO} dnf config-manager --set-enabled epel
      ${SUDO} yum install -y ${BASIC_PACKAGES_CENTOS_8[*]}
      ${SUDO} yum install -y ${ADDITIONAL_PACKAGES_CENTOS_8[*]}
    fi
  fi
}

install_openssl() {
  if [[ -f "${install_prefix}/include/openssl/ssl.h" ]]; then
    info "openssl already installed, skip."
    return 0
  fi

  directory="openssl-OpenSSL_1_1_1k"
  file="OpenSSL_1_1_1k.tar.gz"
  url="https://github.com/openssl/openssl/archive/refs/tags"
  pushd "${tempdir}" || exit
  download_and_untar "${url}" "${file}" "${directory}"
  pushd ${directory} || exit

  ./config --prefix="${install_prefix}" no-shared -fPIC 
  make -j$(nproc)
  make install
  popd || exit
  popd || exit
  rm -rf "${tempdir:?}/${directory:?}" "${tempdir:?}/${file:?}"
  export OPENSSL_ROOT_DIR="${install_prefix}"
}

INTERACTIVE_MACOS=("xsimd", "cmake")
INTERACTIVE_UBUNTU=("cmake" "libssl-dev") # levedb for brpc

install_neug_dependencies() {
  # dependencies package
  if [[ "${OS_PLATFORM}" == *"Darwin"* ]]; then
    brew install ${INTERACTIVE_MACOS[*]}
    install_openssl
  elif [[ "${OS_PLATFORM}" == *"Ubuntu"* ]]; then
    DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC ${SUDO} apt-get install -y ${INTERACTIVE_UBUNTU[*]}
    ${SUDO} sh -c 'echo "fs.aio-max-nr = 1048576" >> /etc/sysctl.conf'
    ${SUDO} sysctl -p /etc/sysctl.conf
  else
    export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/lib/:/lib64${install_prefix}/lib:${install_prefix}/lib64
    if [[ "${OS_VERSION}" -eq "7" ]]; then
      source /opt/rh/devtoolset-10/enable
    fi
    install_openssl
  fi

  if [[ "${CI:-OFF}" == "ON" ]]; then
    install_arrow_from_source
  fi
}

write_env_config() {
  echo "" > ${OUTPUT_ENV_FILE}
  # common
  {
    echo "export NEUG_HOME=${install_prefix}"
    echo "export CMAKE_PREFIX_PATH=/opt/neug/"
    echo "export PATH=${install_prefix}/bin:\$HOME/.local/bin:\$HOME/.cargo/bin:\$PATH"
    echo "export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:\${LD_LIBRARY_PATH}"
    echo "export LIBRARY_PATH=${install_prefix}/lib:${install_prefix}/lib64:\${LIBRARY_PATH}"
    echo "export DYLD_LIBRARY_PATH=${LD_LIBRARY_PATH}:\${DYLD_LIBRARY_PATH}"
  } >> "${OUTPUT_ENV_FILE}"
  {
    if [[ "${OS_PLATFORM}" == *"CentOS"* || "${OS_PLATFORM}" == *"Aliyun"* ]]; then
      if [[ "${OS_VERSION}" -eq "7" ]]; then
        echo "source /opt/rh/devtoolset-10/enable"
        echo "source /opt/rh/rh-python38/enable"
      fi
      echo "export OPENSSL_ROOT_DIR=${install_prefix}"
    fi
  } >> "${OUTPUT_ENV_FILE}"
}

install_deps() {
  init_workspace_and_env
  install_basic_packages
  install_neug_dependencies
  write_env_config
  info "The script has installed all dependencies, don't forget to exec command:\n
  $ source ${OUTPUT_ENV_FILE}
  \nbefore building NeuG."
}

install_deps
