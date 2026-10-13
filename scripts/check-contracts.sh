#!/usr/bin/env bash
# Checks the vendored contracts against the iriai-proto release they claim to
# be: the bundle's SHA-256, every vendored file, and the two source files.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
src="$root/contracts/SOURCE.json"
field() { node -e "const s=require(process.argv[1]);const v=s[process.argv[2]];console.log(Array.isArray(v)?v.join(' '):v)" "$src" "$1"; }
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

curl -fsSL -o "$work/bundle.tar.gz" "$(field bundle)"
echo "$(field sha256)  $work/bundle.tar.gz" | sha256sum -c -
mkdir "$work/x"
tar xzf "$work/bundle.tar.gz" -C "$work/x"
for f in $(field files); do diff -r "$work/x/$f" "$root/contracts/$f"; done

repo="$(field repository)"; commit="$(field commit)"
for f in $(field sources); do
  curl -fsSL "https://raw.githubusercontent.com/$repo/$commit/$f" | diff - "$root/src/contracts/$(basename "$f")"
done
echo "✓ contracts match $(field tag) ($(field sha256 | cut -c1-12)…)"
