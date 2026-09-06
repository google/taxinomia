#!/bin/sh
# Bazel workspace status command (see .bazelrc). Emits the git facts that
# //:taxinomia stamps into core/buildinfo. STABLE_ keys relink the binary
# only when their values change, i.e. once per commit. Outside a git
# checkout nothing is emitted and the binary reports "dev".
commit=$(git rev-parse HEAD 2>/dev/null) || exit 0
echo "STABLE_GIT_COMMIT $commit"
echo "STABLE_GIT_REVISION $(git rev-list --count HEAD)"
echo "STABLE_GIT_DATE $(git log -1 --format=%cs)"
dirty=0
[ -n "$(git status --porcelain --untracked-files=no)" ] && dirty=1
echo "STABLE_GIT_DIRTY $dirty"
