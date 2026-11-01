#!/usr/bin/env bash
# Keep one rolling issue per stream, so that a stream that fails every six hours is one
# open issue with a comment for each failure, and not an issue for each.
#
#   rolling-issue.sh <stream> failure   comment on the stream's open issue, or open one
#   rolling-issue.sh <stream> success   close the stream's open issue, if there is one
#
# It uses plain gh and nothing of the kit, so that it still works when the kit is what
# broke. gh reads its token from GH_TOKEN, which the step sets to the workflow's token
# (it needs `issues: write`). The repository is named in every call, so that it works
# without a checkout.
#
# The issue is the open one that has the label `stream:<stream>` and the title
# `[<stream>] scheduled run failed`. The label is free for people to use too, so the
# title is what says that an issue is this one.

set -euo pipefail

if [ "$#" -ne 2 ] || { [ "$2" != "failure" ] && [ "$2" != "success" ]; }; then
  echo "usage: ${0##*/} <stream> <failure|success>" >&2
  exit 2
fi
stream=$1
outcome=$2

RUN_URL="$GITHUB_SERVER_URL/$GITHUB_REPOSITORY/actions/runs/$GITHUB_RUN_ID"
label="stream:$stream"
title="[$stream] scheduled run failed"

# A listing that fails stops the script (set -e). It is never read as "no open issue",
# because then a failure would open a second issue and a success would leave the first.
listing=$(gh issue list --repo "$GITHUB_REPOSITORY" --label "$label" --state open \
  --limit 100 --json number,title --jq '.[] | "\(.number)\t\(.title)"')
number=""
while IFS=$'\t' read -r found found_title; do
  if [ "$found_title" = "$title" ]; then
    number=$found
    break
  fi
done <<< "$listing"

if [ "$outcome" = "success" ]; then
  if [ -n "$number" ]; then
    gh issue close "$number" --repo "$GITHUB_REPOSITORY" --comment "Recovered: $RUN_URL"
  fi
elif [ -n "$number" ]; then
  gh issue comment "$number" --repo "$GITHUB_REPOSITORY" \
    --body "The run failed again: $RUN_URL"
else
  # --force makes this safe when the label is there already.
  gh label create "$label" --repo "$GITHUB_REPOSITORY" --color B60205 --force \
    --description "Rolling failure issue of the $stream stream"
  gh issue create --repo "$GITHUB_REPOSITORY" --title "$title" --label "$label" \
    --assignee coatless \
    --body "The scheduled run of the stream $stream failed: $RUN_URL

This issue stays open while the stream keeps failing. Each run that fails adds a comment, and the first run that succeeds closes it."
fi
