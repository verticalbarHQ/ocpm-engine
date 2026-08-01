#!/usr/bin/env bash
set -euo pipefail

repo="${1:-$(git rev-parse --show-toplevel)}"
temporary_index="$(mktemp "${TMPDIR:-/tmp}/ocpq-source-tree-index.XXXXXX")"
cleanup() {
  rm -f "$temporary_index"
}
trap cleanup EXIT

# Populate an isolated index from the actual working tree. `git add -A` captures
# staged-only content, unstaged content, deletions, renames, and untracked files
# while respecting ignore rules, without changing the user's real index.
rm -f "$temporary_index"
GIT_INDEX_FILE="$temporary_index" git -C "$repo" read-tree HEAD
GIT_INDEX_FILE="$temporary_index" git -C "$repo" add -A -- .
printf 'git-tree:%s\n' "$(GIT_INDEX_FILE="$temporary_index" git -C "$repo" write-tree)"

