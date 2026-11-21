{
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { nixpkgs, ... }:
    let
      pkgs = nixpkgs.legacyPackages.x86_64-linux;
      # The track's dataset generator. It is std-only (and its Cargo.lock is stale), so plain rustc builds it.
      generate = pkgs.runCommandCC "generate" { nativeBuildInputs = [ pkgs.rustc ]; } ''
        mkdir -p $out/bin
        rustc --edition 2024 -C opt-level=3 -C panic=abort ${./generator}/src/main.rs -o $out/bin/generate
      '';
    in {
      packages.x86_64-linux.generate = generate;
      devShells.x86_64-linux.default = pkgs.mkShell {
        packages = [
          generate
          pkgs.cargo
          pkgs.rustc
          pkgs.clippy
          pkgs.gnumake
          (pkgs.python3.withPackages (p: [ p.polars ]))  # scripts/run_fixtures.py and compare.py
        ];
      };
    };
}
