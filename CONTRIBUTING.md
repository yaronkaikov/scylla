# Contributing to Scylla

## Asking questions or requesting help

Use the [ScyllaDB Community Forum](https://forum.scylladb.com) or the [Slack workspace](http://slack.scylladb.com) for general questions and help.

Join the [Scylla Developers mailing list](https://groups.google.com/g/scylladb-dev) for deeper technical discussions and to discuss your ideas for contributions.

## Reporting an issue

Please use the [issue tracker](https://github.com/scylladb/scylla/issues/) to report issues or to suggest features. Fill in as much information as you can in the issue template, especially for performance problems.

## Contributing code to Scylla

Before you can contribute code to Scylla for the first time, you should sign the [Contributor License Agreement](https://www.scylladb.com/open-source/contributor-agreement/) and send the signed form to cla@scylladb.com. You can then submit your changes as patches to the [scylladb-dev mailing list](https://groups.google.com/forum/#!forum/scylladb-dev) or as a pull request to the [Scylla project on github](https://github.com/scylladb/scylla).
If you need help formatting or sending patches, [check out these instructions](https://github.com/scylladb/scylla/wiki/Formatting-and-sending-patches).

The Scylla C++ source code uses the [Seastar coding style](https://github.com/scylladb/seastar/blob/master/coding-style.md) so please adhere to that in your patches. Note that Scylla code is written with `using namespace seastar`, so should not explicitly add the `seastar::` prefix to Seastar symbols. You will usually not need to add `using namespace seastar` to new source files, because most Scylla header files have `#include "seastarx.hh"`, which does this.

Header files in Scylla must be self-contained, i.e., each can be included without having to include specific other headers first. To verify that your change did not break this property, run `ninja dev-headers`. If you added or removed header files, you must `touch configure.py` first - this will cause `configure.py` to be automatically re-run to generate a fresh list of header files.

For more criteria on what reviewers consider good code, see the [review checklist](https://github.com/scylladb/scylla/blob/master/docs/dev/review-checklist.md).

## Compiler Compatibility

Scylla maintains compatibility with recent versions of GCC and Clang. The periodic compiler build process validates compatibility with the latest stable compiler releases.

### Current Compiler Support

- **GCC**: 10.1.1 or later
- **Clang**: 10.0 or later

### Compiler-Specific Code

When writing code that requires compiler-specific handling:

1. Check existing workarounds in `.github/instructions/cpp.instructions.md`
2. Use compiler version detection macros:
   - `__clang__` - Clang/LLVM compiler
   - `__GNUC__`, `__GNUC_MINOR__` - GCC version
   - `__has_builtin()`, `__has_attribute()` - Feature detection (preferred)
3. Document any compiler-specific flags or patches with comments explaining why they're needed

### Compiler Upgrade Process

When new compiler versions are released (typically every 6 months):

1. Periodic builds are automatically triggered (see `.github/workflows/periodic-compiler-build.yml`)
2. If failures are detected, issues are created for investigation
3. Fixes are documented and applied to the codebase
4. Compiler version is updated in toolchain configuration

### Testing with Development Compilers

To test your changes with the latest compiler versions:

    # Build latest compilers in a container
    ./tools/toolchain/build-latest-compilers \
        --gcc-version 15.0.0 \
        --clang-version 20.0.0 \
        --build-scylla \
        --test-mode dev

    # Validate the build
    ./tools/toolchain/validate-compiler-build.sh --test-mode dev --run-tests

For more details, see [tools/toolchain/README.md](tools/toolchain/README.md#building-with-latest-compilers).
