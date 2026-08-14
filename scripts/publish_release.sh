#!/usr/bin/env bash
#
# Publish the built wheels to a GitHub Release and regenerate the PEP 503 index
# that points at them.
#
# GitHub Releases allow 2 GB per asset, so the ~300 MB platform wheels fit with
# room to spare -- no chunking, no PyPI limit request. What Releases cannot do
# is resolve a package *name*, which is what pip needs. So this script also
# writes a static index under docs/, served by GitHub Pages, whose links point
# back at the release assets. Pages provides the name lookup, Releases provides
# the bytes.
#
# Users then get:
#
#   pip install velox-spark --extra-index-url https://OWNER.github.io/REPO/simple/
#
# Put that URL in the org-wide pip.conf and it collapses back to a bare
# `pip install velox-spark`.
#
# Usage:
#   scripts/build_wheels.sh --x86-jar ... --arm-jar ...
#   scripts/publish_release.sh --repo myorg/velox-spark
#
# Requires the `gh` CLI, authenticated.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST="$ROOT/dist"
DOCS="$ROOT/docs"
REPO=""
DRY_RUN=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --repo)    REPO="$2"; shift 2 ;;
        --dist)    DIST="$2"; shift 2 ;;
        --dry-run) DRY_RUN=1; shift ;;
        -h|--help) sed -n '2,30p' "${BASH_SOURCE[0]}"; exit 0 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

[[ -n "$REPO" ]] || { echo "!! --repo OWNER/NAME is required" >&2; exit 2; }
[[ "$REPO" == */* ]] || { echo "!! --repo must be OWNER/NAME, got '$REPO'" >&2; exit 2; }

OWNER="${REPO%%/*}"
NAME="${REPO##*/}"
PAGES_URL="https://${OWNER}.github.io/${NAME}"

PKG_VERSION="$(grep -m1 '^version = ' "$ROOT/pyproject.toml" | cut -d'"' -f2)"
TAG="v${PKG_VERSION}"

shopt -s nullglob
WHEELS=("$DIST"/*.whl)
shopt -u nullglob
if [[ ${#WHEELS[@]} -eq 0 ]]; then
    echo "!! no wheels in ${DIST}. Run scripts/build_wheels.sh first." >&2
    exit 1
fi

echo "==> repo    ${REPO}"
echo "==> tag     ${TAG}"
echo "==> wheels  ${#WHEELS[@]}"

# A pure-Python wheel on its own means the platform wheels never got built. If
# that reaches the index, Linux users silently install the unaccelerated
# fallback -- exactly the failure this whole package is designed to avoid.
HAS_PLATFORM=0
for wheel in "${WHEELS[@]}"; do
    [[ "$(basename "$wheel")" == *manylinux* ]] && HAS_PLATFORM=1
done
if [[ $HAS_PLATFORM -eq 0 ]]; then
    echo "!! only the pure-Python wheel is present." >&2
    echo "   Publishing it alone gives every Linux user unaccelerated Spark." >&2
    echo "   Rebuild with --x86-jar/--arm-jar, or pass --dry-run if deliberate." >&2
    [[ $DRY_RUN -eq 1 ]] || exit 1
fi

# --- the PEP 503 "simple" index ---------------------------------------------
# Two pages: a project list, and a file list per project. pip reads nothing
# else, so this stays a handful of static HTML with no server behind it.
#
# Every link carries a #sha256 fragment. pip verifies it on download, which
# matters more here than on PyPI: release assets are mutable, so someone can
# replace a wheel under a URL that has already shipped. The hash is what turns
# an install into something reproducible.
write_index() {
    local project_dir="$DOCS/simple/velox-spark"
    mkdir -p "$project_dir"

    cat >"$DOCS/simple/index.html" <<HTML
<!DOCTYPE html>
<html><head><meta name="pypi:repository-version" content="1.0"><title>Simple index</title></head>
<body>
<a href="velox-spark/">velox-spark</a><br>
</body></html>
HTML

    {
        echo '<!DOCTYPE html>'
        echo '<html><head><meta name="pypi:repository-version" content="1.0">'
        echo '<title>Links for velox-spark</title></head>'
        echo '<body>'
        echo '<h1>Links for velox-spark</h1>'
    } >"$project_dir/index.html"

    # Carry forward links from previous releases so older versions stay
    # installable. Dropping them would silently break any pin.
    if [[ -f "$project_dir/index.html.prev" ]]; then
        grep -o '<a href="[^"]*velox_spark[^"]*"[^>]*>[^<]*</a><br>' \
            "$project_dir/index.html.prev" \
            | grep -v "/download/${TAG}/" >>"$project_dir/index.html" || true
    fi

    local wheel base sha
    for wheel in "${WHEELS[@]}"; do
        base="$(basename "$wheel")"
        sha="$(sha256sum "$wheel" | cut -d' ' -f1)"
        printf '<a href="https://github.com/%s/releases/download/%s/%s#sha256=%s" data-requires-python="&gt;=3.9">%s</a><br>\n' \
            "$REPO" "$TAG" "$base" "$sha" "$base" >>"$project_dir/index.html"
    done

    echo '</body></html>' >>"$project_dir/index.html"
    cp "$project_dir/index.html" "$project_dir/index.html.prev"

    # Without this, Pages runs the output through Jekyll, which discards
    # directories beginning with an underscore and rewrites some paths.
    touch "$DOCS/.nojekyll"

    echo "==> wrote ${DOCS}/simple/velox-spark/index.html"
}

write_index

if [[ $DRY_RUN -eq 1 ]]; then
    echo
    echo "dry run: nothing uploaded. Index written to ${DOCS}/simple/."
    exit 0
fi

command -v gh >/dev/null || { echo "!! the gh CLI is required" >&2; exit 1; }

if gh release view "$TAG" --repo "$REPO" >/dev/null 2>&1; then
    echo "==> release ${TAG} exists, replacing assets"
    gh release upload "$TAG" "${WHEELS[@]}" --repo "$REPO" --clobber
else
    echo "==> creating release ${TAG}"
    gh release create "$TAG" "${WHEELS[@]}" \
        --repo "$REPO" \
        --title "velox-spark ${PKG_VERSION}" \
        --notes "Gluten/Velox bundle for Spark 3.5.5.

Install:

    pip install velox-spark --extra-index-url ${PAGES_URL}/simple/

Verify the install with \`velox-spark doctor\`."
fi

echo
echo "Published. Remaining steps:"
echo "  1. Commit and push docs/ -- the index links are live only once Pages"
echo "     serves them."
echo "  2. Enable Pages for ${REPO} from the docs/ folder on the default branch."
echo "  3. Verify:  pip install velox-spark --extra-index-url ${PAGES_URL}/simple/"
