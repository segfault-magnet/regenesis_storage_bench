{
  description = "Fuels rs dev shell";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    #nixpkgs.url = "github:nixos/nixpkgs/nixos-23.05";

    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.rust-analyzer-src.follows = "";
    };

    rust-manifest = {
      url = "http://static.rust-lang.org/dist/channel-rust-1.71.0.toml";
      flake = false;
    };

    fuel = {
      url = "github:fuellabs/fuel.nix";
    };

    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { nixpkgs, fenix, fuel, flake-utils, rust-manifest, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
        };
        rust_deps = with pkgs; [
          fenix.packages.${system}.latest.toolchain
          #(fenix.packages.x86_64-linux.fromManifestFile rust-manifest).toolchain
          cargo-nextest
          cargo-expand
          cargo-udeps
          cargo-feature
          mdbook
          marksman
          nodePackages.markdownlint-cli
          wasm-pack
          nodejs_20
        ];
      in
      {
        devShells.default = pkgs.mkShell {
          nativeBuildInputs = rust_deps ;
        };
      });
}
