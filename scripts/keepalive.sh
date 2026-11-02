#!/usr/bin/env bash
# Keep the scheduled workflows enabled. GitHub disables a scheduled workflow in a
# public repository once the repository has had no commit for 60 days, and the data
# lives in releases, which are not commits, so a repository that only publishes data
# goes quiet. When main's newest commit is 45 days old or older, this makes an empty
# commit on main (the commit of the same tree, on top of the head), which restarts the
# count with 15 days to spare.
#
# It uses plain gh and nothing of the kit, as rolling-issue.sh does. gh reads its token
# from GH_TOKEN, which the step sets to the workflow's token (it needs `contents:
# write`). The repository is named in every call, so that it works without a checkout.
#
# Main is moved without `force`, so the move fails if main changed in the meantime, and
# a later run tries again.

set -euo pipefail

repo=${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is not set}

# The head and its age in whole days. The call is read into a variable first, so that
# a failure stops the script and is never read as an age.
newest=$(gh api "repos/$repo/commits/main" \
  --jq '[.sha, ((now - (.commit.committer.date | fromdateiso8601)) / 86400 | floor)] | @tsv')
IFS=$'\t' read -r head age <<< "$newest"

if [ "$age" -lt 45 ]; then
  echo "main's newest commit is $age days old: nothing to do"
  exit 0
fi

echo "main's newest commit is $age days old: making an empty commit"
tree=$(gh api "repos/$repo/git/commits/$head" --jq .tree.sha)
new=$(gh api "repos/$repo/git/commits" \
  -f "message=Keep the scheduled workflows enabled" -f "tree=$tree" -f "parents[]=$head" \
  --jq .sha)
gh api -X PATCH "repos/$repo/git/refs/heads/main" -f "sha=$new" > /dev/null
echo "main is now $new"
