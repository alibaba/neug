#!/usr/bin/env python3
"""Build and verify a standalone Lite C API archive for the current platform."""

import argparse
import hashlib
import json
import os
import platform
import re
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[1]
BUILD_OPTIONS = {
    "CMAKE_BUILD_TYPE": "Release",
    "NEUG_LITE": "ON",
    "BUILD_COMPILER": "ON",
    "NEUG_PACKAGE_BUILD": "ON",
    "NEUG_NATIVE_ARCH": "OFF",
    "BUILD_PYTHON": "OFF",
    "BUILD_NODEJS": "OFF",
    "BUILD_HTTP_SERVER": "OFF",
    "BUILD_EXTENSIONS": "",
    "NEUG_EXTENSION_CONFIGS": "",
    "WITH_MIMALLOC": "OFF",
    "ENABLE_BACKTRACES": "OFF",
    "BUILD_TEST": "OFF",
    "BUILD_EXAMPLES": "OFF",
    "BUILD_EXECUTABLES": "OFF",
    "BUILD_DOC": "OFF",
    "BUILD_SINGLE_FILE_HEADER": "OFF",
    "BUILD_SHARED_LIBS": "OFF",
    "ENABLE_GCOV": "OFF",
    "ENABLE_ADDRESS_SANITIZER": "OFF",
    "ENABLE_THREAD_SANITIZER": "OFF",
    "ENABLE_UBSAN": "OFF",
    "ENABLE_RUNTIME_CHECKS": "OFF",
}
PAYLOAD = {
    "include/neug/c/neug.h",
    "examples/basic.c",
    "examples/verify.c",
    "examples/CMakeLists.txt",
    "README.md",
    "LICENSE",
}


def run(args, cwd=None, env=None, log=None):
    if log:
        print(f"Running {args[0]} (log: {log})", flush=True)
        with Path(log).open("w") as stream:
            result = subprocess.run(
                args, cwd=cwd, env=env, stdout=stream, stderr=subprocess.STDOUT
            )
        if result.returncode:
            raise RuntimeError(f"Command failed; see {log}")
        return ""
    return subprocess.check_output(args, cwd=cwd, env=env, text=True).strip()


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def read_cache(path):
    return dict(
        re.findall(
            r"^([A-Za-z_][A-Za-z0-9_]*):[^=\r\n]+=(.*)$",
            path.read_text(),
            re.M,
        )
    )


LIBRARIES = {
    "Darwin": ("lib/libneug.dylib", "lib/libneug.dylib"),
    "Linux": ("lib/libneug.so", "lib/libneug.so"),
    "Windows": ("bin/neug.dll", "lib/neug.lib"),
}


def host_arch():
    arch = {"AMD64": "x86_64", "aarch64": "arm64", "ARM64": "arm64"}.get(
        platform.machine(), platform.machine()
    )
    if platform.system() not in LIBRARIES or arch not in ("arm64", "x86_64"):
        raise RuntimeError("Unsupported native build platform")
    return arch


def platform_name(system, arch):
    return f"{dict(Darwin='osx', Linux='linux', Windows='windows')[system]}-{arch}"


def payload(system):
    return PAYLOAD | set(LIBRARIES[system])


def physical_cores(system):
    try:
        if system == "Darwin":
            return int(run(["sysctl", "-n", "hw.physicalcpu"]))
        if system == "Windows":
            return int(
                run(
                    [
                        "powershell",
                        "-NoProfile",
                        "-Command",
                        "(Get-CimInstance Win32_Processor | "
                        "Measure-Object NumberOfCores -Sum).Sum",
                    ]
                )
            )
        rows = run(["lscpu", "-p=CORE,SOCKET"]).splitlines()
        return len({row for row in rows if row and not row.startswith("#")}) or 1
    except (OSError, subprocess.CalledProcessError, ValueError):
        return 1


def rpaths(library):
    return re.findall(
        r"cmd LC_RPATH\s+cmdsize \d+\s+path (.+?) \(offset",
        run(["otool", "-l", str(library)]),
    )


def check_macos_library(library, arch):
    if run(["lipo", "-archs", str(library)]) != arch:
        raise RuntimeError("Library architecture does not match the package")
    names = [
        line.strip().split(" (compatibility version", 1)[0]
        for line in run(["otool", "-L", str(library)]).splitlines()[1:]
    ]
    if not names or names[0] != "@rpath/libneug.dylib":
        raise RuntimeError("Library install name must be @rpath/libneug.dylib")
    dependencies = names[1:]
    unexpected = [
        name
        for name in dependencies
        if not name.startswith(("/usr/lib/", "/System/Library/Frameworks/"))
    ]
    if unexpected:
        raise RuntimeError(f"Non-system dynamic dependencies: {unexpected}")
    if rpaths(library):
        raise RuntimeError("Packaged library must not retain build-time rpaths")
    exported = {
        line.split()[-1][1:]
        for line in run(["nm", "-gU", str(library)]).splitlines()
        if line.split() and line.split()[-1].startswith("_neug_")
    }
    run(["codesign", "--verify", "--strict", str(library)])
    return dependencies, exported


def check_linux_library(library, arch):
    header = run(["readelf", "-h", str(library)])
    machine = "Advanced Micro Devices X86-64" if arch == "x86_64" else "AArch64"
    if not re.search(r"Machine:\s+" + re.escape(machine) + r"\s*$", header, re.M):
        raise RuntimeError("Library architecture does not match the package")
    dynamic = run(["readelf", "-d", str(library)])
    dependencies = re.findall(r"\(NEEDED\).*?\[(.*?)\]", dynamic)
    allowed = {
        "libc.so.6",
        "libm.so.6",
        "libdl.so.2",
        "libpthread.so.0",
        "librt.so.1",
        "ld-linux-x86-64.so.2",
        "ld-linux-aarch64.so.1",
    }
    if not dependencies or set(dependencies) - allowed:
        raise RuntimeError(f"Non-system dynamic dependencies: {dependencies}")
    if re.search(r"\((?:RPATH|RUNPATH)\)", dynamic):
        raise RuntimeError("Packaged library must not retain build-time rpaths")
    if re.findall(r"\(SONAME\).*?\[(.*?)\]", dynamic) != ["libneug.so"]:
        raise RuntimeError("Library SONAME must be libneug.so")
    exported = {
        line.split()[-1].split("@")[0]
        for line in run(["nm", "-D", "--defined-only", str(library)]).splitlines()
        if line.split() and line.split()[-1].startswith("neug_")
    }
    return dependencies, exported


def check_windows_library(library, arch):
    headers = run(["dumpbin", "/nologo", "/headers", str(library)])
    machine = "8664" if arch == "x86_64" else "AA64"
    if not re.search(r"^\s*" + machine + r" machine ", headers, re.M | re.I):
        raise RuntimeError("Library architecture does not match the package")
    dependencies = re.findall(
        r"^\s+([\w.-]+\.dll)\s*$",
        run(["dumpbin", "/nologo", "/dependents", str(library)]),
        re.M | re.I,
    )
    allowed = {
        "kernel32.dll",
        "advapi32.dll",
        "bcrypt.dll",
        "ws2_32.dll",
        "user32.dll",
        "shell32.dll",
        "ole32.dll",
        "oleaut32.dll",
        "ntdll.dll",
        "secur32.dll",
        "crypt32.dll",
        "shlwapi.dll",
        "psapi.dll",
        "version.dll",
        "dbghelp.dll",
        "ucrtbase.dll",
    }
    unexpected = [
        name
        for name in dependencies
        if name.lower() not in allowed
        and not name.lower().startswith(("api-ms-win-core-", "ext-ms-win-"))
    ]
    if not dependencies or unexpected:
        raise RuntimeError(
            f"Non-system dynamic dependencies: {unexpected or dependencies}"
        )
    exported = set(
        re.findall(
            r"^\s+\d+\s+[0-9A-F]+\s+[0-9A-F]+\s+(neug_\w+)(?:\s|$)",
            run(["dumpbin", "/nologo", "/exports", str(library)]),
            re.M,
        )
    )
    return dependencies, exported


def check_library(library, arch, header, system=None):
    system = system or platform.system()
    checker = {
        "Darwin": check_macos_library,
        "Linux": check_linux_library,
        "Windows": check_windows_library,
    }[system]
    dependencies, exported = checker(library, arch)
    declarations = set(re.findall(r"\b(neug_\w+)\s*\(", header.read_text()))
    if declarations != exported:
        raise RuntimeError(
            f"C exports differ: missing={declarations - exported}, "
            f"extra={exported - declarations}"
        )
    includes = re.findall(r"^#include\s+(.+)$", header.read_text(), re.M)
    if includes != ["<stddef.h>", "<stdint.h>"]:
        raise RuntimeError("C header has unexpected include dependencies")
    return dependencies


def linux_glibc_requirement(library):
    versions = re.findall(
        r"\bGLIBC_(\d+(?:\.\d+)+)\b", run(["readelf", "--version-info", str(library)])
    )
    if not versions:
        raise RuntimeError("No glibc version requirement found")
    return max(versions, key=lambda value: tuple(map(int, value.split("."))))


def extract_package(archive, destination):
    # Only regular files/directories under one root; never follow archive links.
    with tarfile.open(archive, "r:gz") as tar:
        members = tar.getmembers()
        roots = set()
        seen = set()
        for member in members:
            path = PurePosixPath(member.name)
            if (
                path.is_absolute()
                or "\\" in member.name
                or ":" in member.name
                or path.as_posix() != member.name.rstrip("/")
                or ".." in path.parts
                or not path.parts
                or not (member.isfile() or member.isdir())
                or path.as_posix().casefold() in seen
            ):
                raise RuntimeError(f"Invalid archive member: {member.name}")
            roots.add(path.parts[0])
            seen.add(path.as_posix().casefold())
        if len(roots) != 1:
            raise RuntimeError("Archive must have one top-level directory")
        for member in members:
            target = destination / member.name
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                with tar.extractfile(member) as source, target.open("wb") as out:
                    shutil.copyfileobj(source, out)
                target.chmod(member.mode & 0o755)
    return destination / roots.pop()


def verify(archive, work_dir):
    arch = host_arch()
    system = platform.system()
    runtime_name, link_name = LIBRARIES[system]
    archive = archive.resolve()
    expected = archive.with_name(archive.name + ".sha256").read_text().split()[0]
    if sha256(archive) != expected:
        raise RuntimeError("Archive SHA256 mismatch")
    work_dir.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="verify-", dir=work_dir))
    package = extract_package(archive, work)
    metadata = json.loads((package / "manifest.json").read_text())
    actual = {
        p.relative_to(package).as_posix() for p in package.rglob("*") if p.is_file()
    }
    if actual != payload(system) | {"manifest.json"}:
        raise RuntimeError(f"Unexpected package contents: {actual}")
    if set(metadata["files"]) != payload(system):
        raise RuntimeError("Manifest file list differs from the package contract")
    for name, digest in metadata["files"].items():
        if sha256(package / name) != digest:
            raise RuntimeError(f"File SHA256 mismatch: {name}")
    if metadata["platform"] != platform_name(system, arch):
        raise RuntimeError("This package does not match the verification host")
    dependencies = check_library(
        package / runtime_name, arch, package / "include/neug/c/neug.h", system
    )
    if dependencies != metadata["system_dependencies"]:
        raise RuntimeError("Dependency manifest does not match the library")

    if system == "Linux" and metadata["glibc_minimum"] != linux_glibc_requirement(
        package / runtime_name
    ):
        raise RuntimeError("glibc requirement differs from the manifest")

    # Do not let a developer's installed NeuG or compiler search paths mask a
    # missing dependency. Tests use only the extracted package and OS SDK.
    env = os.environ.copy()
    for key in list(env):
        if key.startswith("DYLD_") or key in {
            "CPATH",
            "C_INCLUDE_PATH",
            "CPLUS_INCLUDE_PATH",
            "LIBRARY_PATH",
            "LD_LIBRARY_PATH",
            "LD_PRELOAD",
            "CMAKE_PREFIX_PATH",
            "CMAKE_INCLUDE_PATH",
            "CMAKE_LIBRARY_PATH",
            "CMAKE_FRAMEWORK_PATH",
            "CMAKE_TOOLCHAIN_FILE",
            "CC",
            "CXX",
            "CFLAGS",
            "CPPFLAGS",
            "CXXFLAGS",
            "LDFLAGS",
            "SDKROOT",
        }:
            del env[key]
    summary = {
        "archive": str(archive),
        "sha256": expected,
        "host": {
            "platform": platform_name(system, arch),
            "os_version": platform.version(),
        },
        "tests": [],
    }
    basic_build = work / "basic-build"
    run(
        [
            "cmake",
            "-S",
            str(package / "examples"),
            "-B",
            str(basic_build),
            "-DCMAKE_PREFIX_PATH=" + str(package),
            "-DNEUG_VERIFY_PACKAGE=ON",
            "-DCMAKE_BUILD_TYPE=Release",
        ]
        + (["-G", "Ninja"] if system == "Windows" else []),
        cwd=work,
        env=env,
        log=work / "basic-configure.log",
    )
    cache = read_cache(basic_build / "CMakeCache.txt")
    if (
        Path(cache.get("NEUG_INCLUDE_DIR", "")).resolve() != package / "include"
        or Path(cache.get("NEUG_LIBRARY", "")).resolve() != package / link_name
    ):
        raise RuntimeError("Example resolved files outside the extracted package")
    run(
        [
            "cmake",
            "--build",
            str(basic_build),
            "--config",
            "Release",
            "--parallel",
            "1",
        ],
        cwd=work,
        env=env,
        log=work / "basic-build.log",
    )
    runtime_env = env.copy()
    suffix = ".exe" if system == "Windows" else ""
    if system == "Windows":
        if (
            Path(cache.get("NEUG_RUNTIME_LIBRARY", "")).resolve()
            != package / runtime_name
        ):
            raise RuntimeError("Example resolved a DLL outside the extracted package")
        if sha256(basic_build / "neug.dll") != sha256(package / runtime_name):
            raise RuntimeError("Example did not deploy the packaged DLL")
        # Exclude vcpkg/Visual Studio and all other developer runtime DLL paths.
        windows = Path(env.get("SystemRoot") or env["SYSTEMROOT"])
        runtime_env["PATH"] = os.pathsep.join(map(str, [windows / "System32", windows]))
    basic_log = work / "basic-run.log"
    run(
        [str(basic_build / ("neug_c_example" + suffix))],
        cwd=work,
        env=runtime_env,
        log=basic_log,
    )
    if "1 | O'Connor" not in basic_log.read_text().splitlines():
        raise RuntimeError("Basic example returned unexpected output")
    summary["tests"].append({"case": "cmake-basic-example", "status": "passed"})
    for language, target in (("c", "verify_c"), ("c++", "verify_cpp")):
        executable = basic_build / (target + suffix)
        for phase in ("write", "read"):
            log = work / (executable.name + "-" + phase + ".log")
            run(
                [
                    str(executable),
                    str(work / (executable.name + "-db")),
                    metadata["version"],
                    phase,
                ],
                cwd=work,
                env=runtime_env,
                log=log,
            )
            if f"PASS: C API package {phase}" not in log.read_text():
                raise RuntimeError(f"Missing verification result: {log}")
            summary["tests"].append(
                {"language": language, "phase": phase, "status": "passed"}
            )
    summary["system_dependencies"] = dependencies
    (work / "verification.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(f"Verified extracted package: {package}", flush=True)
    print(f"Verification report: {work / 'verification.json'}", flush=True)
    return work


def build(args):
    # Capture the input tree before CMake applies the repository's bundled
    # third-party patches. Those expected build steps dirty submodules.
    source_commit = run(["git", "rev-parse", "HEAD"], cwd=ROOT)
    source_has_local_changes = bool(run(["git", "status", "--porcelain"], cwd=ROOT))
    arch = host_arch()
    build_dir = args.build_dir.resolve()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    logs = Path(tempfile.mkdtemp(prefix="build-", dir=output_dir))
    system = platform.system()
    jobs = min(args.jobs, physical_cores(system))
    if jobs < 1:
        raise RuntimeError("jobs must be positive")
    options = dict(BUILD_OPTIONS)
    if system == "Darwin":
        options["CMAKE_OSX_ARCHITECTURES"] = arch
        options["CMAKE_OSX_DEPLOYMENT_TARGET"] = args.deployment_target
    else:
        # The C API does not use protobuf's optional gzip streams. Avoid
        # introducing a separate zlib dependency on Linux and Windows.
        options["protobuf_WITH_ZLIB"] = "OFF"
    if system == "Linux":
        options["CMAKE_SHARED_LINKER_FLAGS"] = "-static-libstdc++ -static-libgcc"
        options["CMAKE_SKIP_RPATH"] = "ON"
    if system == "Windows":
        options["CMAKE_MSVC_RUNTIME_LIBRARY"] = "MultiThreaded"
        options["CMAKE_RUNTIME_OUTPUT_DIRECTORY"] = (build_dir / "src").as_posix()
        options["CMAKE_ARCHIVE_OUTPUT_DIRECTORY"] = (build_dir / "src").as_posix()
    print(
        f"Building {platform_name(system, arch)} Lite C API with {jobs} parallel jobs",
        flush=True,
    )
    run(
        ["cmake", "-S", str(ROOT), "-B", str(build_dir)]
        + [f"-D{key}={value}" for key, value in options.items()]
        + (["-G", "Ninja"] if system == "Windows" else []),
        log=logs / "configure.log",
    )
    cache = read_cache(build_dir / "CMakeCache.txt")
    for key, value in options.items():
        if cache.get(key) != value:
            raise RuntimeError(f"Unexpected build setting: {key}={cache.get(key)}")
    run(
        [
            "cmake",
            "--build",
            str(build_dir),
            "--target",
            "neug",
            "--config",
            "Release",
            "--parallel",
            str(jobs),
        ],
        log=logs / "build.log",
    )
    version = (ROOT / "NEUG_VERSION").read_text().strip()
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+[A-Za-z0-9.+-]*", version):
        raise RuntimeError("Invalid release version")
    name = f"neug-c-api-{version}-lite-{platform_name(system, arch)}"
    stage = logs / name
    (stage / "include/neug/c").mkdir(parents=True)
    (stage / "lib").mkdir()
    (stage / "examples").mkdir()
    shutil.copyfile(ROOT / "include/neug/c/neug.h", stage / "include/neug/c/neug.h")
    runtime_name, link_name = LIBRARIES[system]
    library = stage / runtime_name
    library.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(build_dir / "src" / library.name, library)
    if system == "Darwin":
        run(["install_name_tool", "-id", "@rpath/libneug.dylib", str(library)])
        for path in rpaths(library):
            run(["install_name_tool", "-delete_rpath", path, str(library)])
        run(["strip", "-x", str(library)])
        run(["codesign", "--force", "--sign", "-", str(library)])
    elif system == "Linux":
        run(["strip", "--strip-unneeded", str(library)])
    else:
        shutil.copyfile(build_dir / "src/neug.lib", stage / link_name)
    library.chmod(0o755)
    dependencies = check_library(library, arch, stage / "include/neug/c/neug.h", system)
    for filename in ("basic.c", "verify.c", "CMakeLists.txt"):
        shutil.copyfile(ROOT / "examples/c" / filename, stage / "examples" / filename)
    shutil.copyfile(ROOT / "examples/c/README.md", stage / "README.md")
    shutil.copyfile(ROOT / "LICENSE", stage / "LICENSE")
    metadata = {
        "version": version,
        "variant": "lite",
        "platform": platform_name(system, arch),
        "c_api_version": int(
            re.search(
                r"#define NEUG_C_API_VERSION (\d+)",
                (stage / "include/neug/c/neug.h").read_text(),
            ).group(1)
        ),
        "source_commit": source_commit,
        "source_has_local_changes": source_has_local_changes,
        "build_options": options,
        "system_dependencies": dependencies,
        "files": {name: sha256(stage / name) for name in sorted(payload(system))},
    }
    if system == "Darwin":
        metadata["deployment_target"] = args.deployment_target
    elif system == "Linux":
        metadata["glibc_minimum"] = linux_glibc_requirement(library)
    (stage / "manifest.json").write_text(json.dumps(metadata, indent=2) + "\n")
    archive = logs / (name + ".tar.gz")
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(stage, arcname=name)
    checksum = archive.with_name(archive.name + ".sha256")
    checksum.write_text(f"{sha256(archive)}  {archive.name}\n")
    verify(archive, output_dir)
    # Only promote an archive after its extracted contents pass verification.
    shutil.copyfile(archive, output_dir / archive.name)
    shutil.copyfile(checksum, output_dir / checksum.name)
    print(f"Package: {output_dir / archive.name}", flush=True)
    print(f"SHA256: {output_dir / checksum.name}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    pack = commands.add_parser("build", help="Build, package, and verify native Lite")
    pack.add_argument("--build-dir", type=Path, default=ROOT / "build")
    pack.add_argument("--output-dir", type=Path, default=ROOT / "build/c-api-dist")
    pack.add_argument("--jobs", type=int, default=2)
    pack.add_argument("--deployment-target", default="11.0")
    check = commands.add_parser(
        "verify", help="Verify an existing archive and checksum"
    )
    check.add_argument("archive", type=Path)
    check.add_argument("--work-dir", type=Path, default=ROOT / "build/c-api-dist")
    args = parser.parse_args()
    if args.command == "build":
        build(args)
    else:
        verify(args.archive, args.work_dir.resolve())


if __name__ == "__main__":
    main()
