# Third-party Libraries

NeuG relies on several third-party libraries to provide its functionality. In some cases, we apply patches to ensure compatibility and stability across different platforms.

### LevelDB

We use LevelDB version 1.23, with modifications to address compiler warnings related to variable usage.

### Protobuf

We use Protobuf version 3.21.9, including the changes from [this pull request](https://github.com/protocolbuffers/protobuf/pull/10188).