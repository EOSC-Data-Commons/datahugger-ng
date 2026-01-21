{ pkgs, ... }:
{
  languages.rust.enable = true;

  packages = [
    # coverage testing
    pkgs.cargo-tarpaulin
    # installers
    pkgs.cargo-dist
  ];

  enterTest = ''
    cargo test --all
  '';

  scripts.test-cli-integration.exec = ''
    # Build the CLI for integration tests
    cargo build --release
    export PATH="$PWD/target/release:$PATH"

    # Run CLI integration tests
    bash tests/cli-integration.sh
  '';
}
