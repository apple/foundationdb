# FoundationDB Swift Bindings

Swift language bindings for [FoundationDB](https://www.foundationdb.org/).

## Requirements

- Swift 5.9+
- FoundationDB client libraries installed

## Installation

### Swift Package Manager

Add this to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/apple/foundationdb", from: "7.3.0")
]
```

**Note:** Since this package is in a subdirectory, you'll need to specify the path when building:

```bash
# Clone the repo
git clone https://github.com/apple/foundationdb.git
cd foundationdb/bindings/swift

# Build the Swift package
swift build

# Or add as local dependency
.package(path: "path/to/foundationdb/bindings/swift")
```
