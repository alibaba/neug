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

<#
.SYNOPSIS
  Install NeuG build dependencies on Windows.

.DESCRIPTION
  Windows counterpart of scripts/install_deps.sh. Instead of apt/yum/brew it
  relies on vcpkg (classic mode) for third-party libraries that are not
  bundled under third_party/, and on Visual Studio Build Tools for the MSVC
  toolchain (cl/link/cmake/ninja).

  On success it writes $HOME\.neug_env.ps1 (counterpart of ~/.neug_env);
  dot-source it before building:

      . $HOME\.neug_env.ps1

  All third-party libraries (OpenSSL, etc.) are installed under
  $InstallDir\vcpkg\installed\$Triplet\. The vcpkg checkout itself
  lives at $InstallDir\vcpkg\.

.PARAMETER InstallDir
  Root directory for all NeuG third-party dependencies. vcpkg is cloned
  into $InstallDir\vcpkg and packages are installed under
  $InstallDir\vcpkg\installed\$Triplet\. Defaults to C:\neug-deps.

.PARAMETER VcpkgRoot
  Explicit vcpkg checkout directory. Overrides auto-detection from
  $InstallDir\vcpkg, $env:VCPKG_ROOT, and common locations.

.PARAMETER Triplet
  vcpkg target triplet. Must stay in sync with build_windows.bat and the
  CRT configuration in CMakeLists.txt (/MD == *-static-md).

.PARAMETER CN
  Use mirrors inside mainland China where possible (counterpart of --cn).

.PARAMETER DebugMode
  Print extra diagnostic output (counterpart of --debug).
#>
[CmdletBinding()]
param(
    [string]$InstallDir = "C:\neug-deps",
    [string]$VcpkgRoot = "",
    [string]$Triplet = "x64-windows-static-md",
    [switch]$CN,
    [switch]$DebugMode
)

$ErrorActionPreference = "Stop"

# Libraries taken from vcpkg. Everything else (protobuf, gflags, glog,
# yaml-cpp, ...) is vendored under third_party/ and built by CMake.
$VcpkgPackages = @("openssl")

$OutputEnvFile = Join-Path $HOME ".neug_env.ps1"

# ---------------------------------------------------------------------------
# logging helpers (mirror info/err/warning/debug of install_deps.sh)
# ---------------------------------------------------------------------------
function Info($msg)    { Write-Host $msg -ForegroundColor Blue }
function Err($msg)     { Write-Host $msg -ForegroundColor Red }
function Warning($msg) { Write-Host $msg -ForegroundColor Yellow }
function DebugLog($msg) {
    if ($DebugMode) { Write-Host "[DEBUG] $msg" -ForegroundColor Red }
}

# ---------------------------------------------------------------------------
# platform gate (counterpart of get_os_version)
# ---------------------------------------------------------------------------
if (-not ($env:OS -eq "Windows_NT")) {
    Err "This script only supports Windows. Use scripts/install_deps.sh on Linux/macOS."
    exit 1
}
DebugLog "OS: $([System.Environment]::OSVersion.VersionString), Triplet: $Triplet"

# ---------------------------------------------------------------------------
# Visual Studio Build Tools (MSVC + CMake + Ninja)
# ---------------------------------------------------------------------------
function Find-VcVarsAll {
    # Preferred: query via vswhere (installed together with any VS >= 15.2).
    $vswhere = Join-Path ${env:ProgramFiles(x86)} "Microsoft Visual Studio\Installer\vswhere.exe"
    if (Test-Path $vswhere) {
        $installPath = & $vswhere -latest -products * `
            -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 `
            -property installationPath 2>$null | Select-Object -First 1
        if ($installPath) {
            $bat = Join-Path $installPath "VC\Auxiliary\Build\vcvarsall.bat"
            if (Test-Path $bat) { return $bat }
        }
    }
    # Fallback: well-known install locations.
    $candidates = @(
        (Join-Path ${env:ProgramFiles(x86)} "Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvarsall.bat"),
        (Join-Path $env:ProgramFiles "Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvarsall.bat"),
        (Join-Path $env:ProgramFiles "Microsoft Visual Studio\2022\Professional\VC\Auxiliary\Build\vcvarsall.bat"),
        (Join-Path $env:ProgramFiles "Microsoft Visual Studio\2022\Enterprise\VC\Auxiliary\Build\vcvarsall.bat")
    )
    foreach ($c in $candidates) {
        if (Test-Path $c) { return $c }
    }
    return $null
}

$VcVarsAll = Find-VcVarsAll
if (-not $VcVarsAll) {
    Err ("Visual Studio Build Tools (C++ workload) not found.`n" +
         "Install them first, e.g.:`n" +
         "  winget install Microsoft.VisualStudio.2022.BuildTools --override `
`"--add Microsoft.VisualStudio.Workload.VCTools --includeRecommended`"")
    exit 1
}
Info "Found MSVC environment script: $VcVarsAll"

# ---------------------------------------------------------------------------
# Python 3 (required by BUILD_PYTHON=ON and pre-commit tooling)
# ---------------------------------------------------------------------------
$PythonExe = $null
$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if ($pythonCmd) {
    # The WindowsApps store alias is a stub that opens the Microsoft Store;
    # filter it out.
    if ($pythonCmd.Source -notmatch "WindowsApps") {
        $PythonExe = $pythonCmd.Source
    }
}
if (-not $PythonExe) {
    Err ("Python 3 not found on PATH.`n" +
         "Install it first, e.g.:  winget install Python.Python.3.11")
    exit 1
}
$pythonVersion = & $PythonExe --version 2>&1
Info "Found Python: $PythonExe ($pythonVersion)"

# ---------------------------------------------------------------------------
# vcpkg: locate, bootstrap when missing (counterpart of install_openssl/
# install_curl source builds on Linux)
# ---------------------------------------------------------------------------
function Find-VcpkgRoot {
    # 1. Explicit -VcpkgRoot parameter
    if ($VcpkgRoot -and (Test-Path (Join-Path $VcpkgRoot ".vcpkg-root"))) {
        return (Resolve-Path $VcpkgRoot).Path
    }
    # 2. $env:VCPKG_ROOT (e.g. pre-set on GitHub runners)
    if ($env:VCPKG_ROOT -and (Test-Path (Join-Path $env:VCPKG_ROOT ".vcpkg-root"))) {
        return $env:VCPKG_ROOT
    }
    # 3. Candidate locations: $InstallDir\vcpkg, $HOME\vcpkg, C:\vcpkg
    $candidates = @(
        (Join-Path $InstallDir "vcpkg"),
        (Join-Path $HOME "vcpkg"),
        "C:\vcpkg"
    )
    foreach ($c in $candidates) {
        if (Test-Path (Join-Path $c ".vcpkg-root")) { return $c }
    }
    return $null
}

$ResolvedVcpkgRoot = Find-VcpkgRoot
if (-not $ResolvedVcpkgRoot) {
    $ResolvedVcpkgRoot = Join-Path $InstallDir "vcpkg"
    if (-not (Test-Path $InstallDir)) {
        New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
    }
    Warning "vcpkg not found; cloning into $ResolvedVcpkgRoot"
    $gitCmd = Get-Command git -ErrorAction SilentlyContinue
    if (-not $gitCmd) {
        Err "git is required to bootstrap vcpkg. Install it first:  winget install Git.Git"
        exit 1
    }
    $vcpkgRepo = "https://github.com/microsoft/vcpkg.git"
    if ($CN) {
        # GitHub is reachable from mainland China but often slow; keep the
        # official repo and let the user override with a mirror if needed.
        Warning "-CN: cloning vcpkg from github.com; set up a git mirror manually if this is too slow."
    }
    git clone --depth 1 $vcpkgRepo $ResolvedVcpkgRoot
    if ($LASTEXITCODE -ne 0) {
        Err "Failed to clone vcpkg."
        exit 1
    }
}

$VcpkgExe = Join-Path $ResolvedVcpkgRoot "vcpkg.exe"
if (-not (Test-Path $VcpkgExe)) {
    Info "Bootstrapping vcpkg..."
    & (Join-Path $ResolvedVcpkgRoot "bootstrap-vcpkg.bat") -disableMetrics
    if (-not (Test-Path $VcpkgExe)) {
        Err "vcpkg bootstrap failed."
        exit 1
    }
}
Info "Using vcpkg: $VcpkgExe"

$InstallPrefix = Join-Path $ResolvedVcpkgRoot "installed\$Triplet"

# ---------------------------------------------------------------------------
# install packages (counterpart of install_neug_dependencies)
# ---------------------------------------------------------------------------
foreach ($pkg in $VcpkgPackages) {
    Info "Installing ${pkg}:${Triplet} via vcpkg"
    & $VcpkgExe install "${pkg}:${Triplet}"
    if ($LASTEXITCODE -ne 0) {
        Err "vcpkg failed to install ${pkg}:${Triplet}"
        exit 1
    }
}

# ---------------------------------------------------------------------------
# write env config (counterpart of write_env_config)
# ---------------------------------------------------------------------------
function Write-EnvConfig {
    $lines = @(
        "# NeuG build environment - generated by scripts/install_deps.ps1",
        "# Windows counterpart of ~/.neug_env; load with:  . `$HOME\.neug_env.ps1",
        "#",
        "# All third-party libraries are under $InstallDir\vcpkg\installed\$Triplet\",
        "`$env:NEUG_DEPS_DIR       = '$InstallDir'",
        "`$env:NEUG_HOME            = '$InstallPrefix'",
        "`$env:VCPKG_ROOT           = '$ResolvedVcpkgRoot'",
        "`$env:VCPKG_TARGET_TRIPLET = '$Triplet'",
        "`$env:CMAKE_TOOLCHAIN_FILE = '$ResolvedVcpkgRoot\scripts\buildsystems\vcpkg.cmake'",
        "`$env:CMAKE_PREFIX_PATH    = '$InstallPrefix'",
        "`$env:OPENSSL_ROOT_DIR     = '$InstallPrefix'",
        "`$env:PYTHON_EXE           = '$PythonExe'",
        "`$env:NEUG_VCVARSALL       = '$VcVarsAll'",
        "# Windows resolves DLLs via PATH (counterpart of LD_LIBRARY_PATH).",
        "`$env:Path                 = '$InstallPrefix\bin;' + `$env:Path"
    )
    Set-Content -Path $OutputEnvFile -Value ($lines -join "`r`n") -Encoding UTF8
}

Write-EnvConfig

Info ("Dependencies installed under $InstallDir`n" +
      "  vcpkg checkout:     $ResolvedVcpkgRoot`n" +
      "  installed packages: $InstallPrefix`n`n" +
      "The environment config has been written to $OutputEnvFile`n" +
      "Don't forget to load it before building:`n`n" +
      "  . `$HOME\.neug_env.ps1`n`n" +
      "and run vcvarsall / build_windows.bat for the MSVC toolchain.")
