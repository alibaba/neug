'use strict';

const OS_MAP = { linux: 'linux', darwin: 'osx' };
const ARCH_MAP = { x64: 'x86_64', arm64: 'arm64' };

const os = OS_MAP[process.platform];
const arch = ARCH_MAP[process.arch];
if (!os || !arch) {
  throw new Error(`NeuG: unsupported platform ${process.platform}-${process.arch}`);
}

const optionalPkg = `@graphscope-neug/${os}-${arch}`;

let binding;
try {
  binding = require(optionalPkg);
} catch (err) {
  throw new Error(
    `NeuG: failed to load platform package "${optionalPkg}".\n` +
    `Original error: ${err.message}`
  );
}

module.exports = binding;
