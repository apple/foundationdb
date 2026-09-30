{
  description = "A dev environment for the crate foundationdb-rs";
  inputs = {
    nixpkgs.url = "https://channels.nixos.org/nixpkgs-unstable/nixexprs.tar.zst";
    rust-overlay.url = "github:oxalica/rust-overlay";
    rust-overlay.inputs.nixpkgs.follows = "nixpkgs";
    fdb-overlay.url = "github:foundationdb-rs/overlay";
    fdb-overlay.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = all@{ self, nixpkgs, fdb-overlay, rust-overlay, ... }: {
    # Utilized by `nix develop`
    devShells.x86_64-linux.default =
      let
        overlays = [ (import rust-overlay) fdb-overlay.overlays.default ];
        pkgs = import nixpkgs {
          inherit overlays;
          system = "x86_64-linux";
        };
        # Toolchain pinned to the workspace MSRV via ./rust-toolchain.toml
        rustToolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
      in
      with pkgs;
      mkShell {
        buildInputs = [
          # bindgen part
          clang
          llvmPackages.libclang
          llvmPackages.libcxxClang
          pkg-config

          # Rust part
          cargo-expand
          cargo-edit
          cargo-msrv
          # uncomment when https://github.com/NixOS/nixpkgs/issues/354058 is fixed
          # cargo-llvm-cov
          cargo-audit
          rustToolchain

          git-cliff
          release-plz

          # FDB part
          fdbserver74
          libfdb74

          # bindingTester part
          python3
          virtualenv
        ];

        # https://github.com/NixOS/nixpkgs/issues/52447#issuecomment-853429315
        BINDGEN_EXTRA_CLANG_ARGS = "-isystem ${llvmPackages.libclang.lib}/lib/clang/${lib.getVersion clang}/include";
        LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib";

        FDB_CLIENT_LIB_PATH = "${libfdb74}/include";
        LD_LIBRARY_PATH = "${libfdb74}/include";

        # To import with Intellij IDEA
        RUST_TOOLCHAIN_PATH = "${rustToolchain}/bin";
        RUST_SRC_PATH = "${rustToolchain}/lib/rustlib/src/rust/library";

        RUST_BACKTRACE = "1";
      };
  };
}
