{
  description = "extindex";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      nixpkgs,
      rust-overlay,
      flake-utils,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          config = {
            allowUnfree = true;
          };
          overlays = [ (import rust-overlay) ];
        };
      in
      {
        devShells = {
          name = "extindex";

          default = pkgs.mkShell {
            buildInputs = with pkgs; [
              clang

              (rust-bin.stable.latest.default.override {
                extensions = [ "rust-src" ];
                targets = [ ];
              })
              rust-analyzer
              cargo-outdated
            ];
          };
        };
      }
    );
}
