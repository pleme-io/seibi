{
  description = "seibi — infrastructure maintenance toolkit";

  inputs.nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";

  outputs = { self, nixpkgs, ... }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" "x86_64-darwin" ];
      eachSystem = f:
        nixpkgs.lib.genAttrs systems (system:
          f (import nixpkgs { inherit system; })
        );
    in
    {
      packages = eachSystem (pkgs: {
        default = pkgs.rustPlatform.buildRustPackage {
          pname = "seibi";
          version = "0.1.0";
          src = pkgs.lib.cleanSource ./.;
          cargoLock.lockFile = ./Cargo.lock;

          buildInputs = pkgs.lib.optionals pkgs.stdenv.hostPlatform.isDarwin (
            if pkgs ? apple-sdk
            then [ pkgs.apple-sdk ]
            else
              with pkgs.darwin.apple_sdk.frameworks;
              [ Security SystemConfiguration ]
          );

          meta.mainProgram = "seibi";
        };
      });

      devShells = eachSystem (pkgs: {
        default = pkgs.mkShellNoCC {
          packages = with pkgs; [
            cargo
            rustc
            rust-analyzer
            clippy
            rustfmt
          ];
        };
      });

      overlays.default = final: prev: {
        seibi = self.packages.${final.system}.default;
      };
    };
}
