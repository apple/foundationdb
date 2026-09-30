# Contributing to FoundationDB Rust

Thank you for your interest in contributing to the FoundationDB Rust client! This guide will help you get started.

## Getting Started

### Prerequisites

You'll need:
- Rust 1.85.1 or later
- FoundationDB client libraries (for runtime, not required for compilation with our setup)

### Quick Development Setup

We provide cargo aliases that use embedded headers, so you can compile without having FoundationDB installed:

```shell
# Build/test/check with latest FDB version (7.4)
cargo build-fdb-latest
cargo test-fdb-latest
cargo check-fdb-latest
cargo clippy-fdb-latest
```

These commands will build the `foundationdb` crate with embedded headers for FDB 7.4.

### Running a Local FoundationDB Cluster

The easiest way to run a local FDB cluster for testing:

```shell
# Start a single-node cluster in Docker
docker run -p 4500:4500 --name fdb -it --rm -d foundationdb/foundationdb:7.4.3
docker exec fdb fdbcli --exec "configure new single memory"
```

## Development with Nix

If you use Nix, we provide a `flake.nix` for a complete development environment:

```shell
# Enter the development shell
nix develop

# The shell includes all necessary dependencies and tools
```

For NixOS users, add this to your `configuration.nix` for the FDB cluster file:

```nix
{
  environment.etc."foundationdb/fdb.cluster" = {
    mode = "0555";
    text = ''
      docker:docker@127.0.0.1:4500
    '';
  };
}
```

## Project Structure

- `foundationdb/` - High-level Rust client API
- `foundationdb-sys/` - Low-level C API bindings
- `foundationdb-gen/` - Code generator for FDB options and types
- `foundationdb-tuple/` - Tuple layer implementation
- `foundationdb-macros/` - Procedural macros
- `foundationdb-bindingtester/` - Official binding tester implementation
- `foundationdb-simulation/` - Simulation testing framework

## Testing

### Unit Tests

Run tests for the main crate:
```shell
cargo test-fdb-latest
```

### Binding Tester

We use the official FoundationDB binding tester to ensure correctness:

```shell
# Build the binding tester
cargo build -p bindingtester

# Setup the binding tester environment (downloads FDB source and configures Python bindings)
./scripts/setup_bindingtester.sh target/debug/bindingtester

# Run the binding tester suite (requires FDB cluster running)
./scripts/run_bindingtester.sh

# Run with more iterations (e.g., 100 iterations)
./scripts/run_bindingtester.sh 100
```

The binding tester runs automatically in CI when a PR has the `correctness` label.

### Leader-election simulation campaigns

The leader-election simulator runs a smoke campaign of 1 worker x 10 seeds on
ordinary pull requests and pushes to `main`. Changes to the leader-election or
RankedRegister recipes, simulation code, the simulation runner, or this
workflow automatically run an extended campaign of 4 workers x 50 seeds.

The full campaign runs nightly at 02:00 UTC, when manually dispatched, and on
pull requests carrying the `leader-election-correctness` label. It uses 4
workers x 200 seeds. Apply that label when a change needs the full
correctness campaign before the nightly run.

## Code Style

- Run `cargo fmt` before committing
- Run `cargo clippy-fdb-latest` to check for common issues
- Follow Rust naming conventions and idioms

## Submitting Changes

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Ensure tests pass (`cargo test-fdb-latest`)
5. Run clippy (`cargo clippy-fdb-latest`)
6. Format your code (`cargo fmt`)
7. Commit your changes with a semantic commit message (see below)
8. Push to your fork
9. Open a Pull Request

### Commit Message Convention

We follow [Conventional Commits](https://www.conventionalcommits.org/) to generate clean changelogs:

```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, missing semicolons, etc.)
- `refactor`: Code changes that neither fix bugs nor add features
- `perf`: Performance improvements
- `test`: Adding or updating tests
- `build`: Changes to build system or dependencies
- `ci`: CI/CD configuration changes
- `chore`: Other changes that don't modify src or test files

**Examples:**
```
feat(tuple): add support for nested tuples

fix(transaction): handle network errors in retry loop

docs: update binding tester instructions

chore(deps): bump foundationdb-sys to 0.9.2
```

### Pull Request Guidelines

- Provide a clear description of the changes
- Include tests for new functionality
- Update documentation as needed
- Ensure CI passes
- For significant changes, consider opening an issue first to discuss

## Correctness

This project prioritizes correctness. We run extensive binding tests to ensure compatibility with the official FoundationDB bindings. The binding tester runs thousands of test seeds regularly in CI.

## Getting Help

- Join our [Discord server](https://discord.gg/zkgtbtFfWY)
- Open an issue on [GitHub](https://github.com/foundationdb-rs/foundationdb-rs/issues)
- Check existing issues and pull requests

## License

By contributing, you agree that your contributions will be licensed under the same dual license as the project (MIT/Apache-2.0).
