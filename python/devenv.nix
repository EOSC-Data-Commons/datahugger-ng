{ pkgs, ... }: {
  languages.rust.enable = true;
  languages.python = {
    enable = true;
    version = "3.12.12";
    venv = {
      enable = true;
    };
    uv = {
      enable = true;
      sync = {
        enable = true;
        allPackages = true;
      };
    };
  };

  packages = [
    # coverage testing
    pkgs.cargo-tarpaulin
    # installers
    pkgs.cargo-dist
    # rust python-bindings
    pkgs.maturin
  ];

  git-hooks.hooks = {
    rustfmt.enable = true;
    clippy.enable = true;
  };

  enterTest = ''
    cargo test --all
  '';
}
