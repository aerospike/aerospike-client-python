#!/bin/bash

set -e
set -x

# This script assumes you:
# Have authenticated to Github
# and have cloned and cd'ed into the Python client repo's root directory

CHANGE_TYPE=$1
# Optional, only required if manual-override is passed as the change type
RELEASE_VERSION_TO_OVERRIDE=$2

if [[ -z "$DRY_RUN" ]]; then
    git config --global user.email "41898282+github-actions[bot]@users.noreply.github.com"
    git config --global user.name "github-actions[bot]"
fi

update_release_version_in_repo () {
    verb=$1
    new_version=$2
    echo "$new_version" > VERSION
    git add -u
    # TODO: double verify that github actions bot user info is configured
    git commit -m "$verb version to $new_version [skip ci]"
    if [[ -z "$DRY_RUN" ]]; then
        git push
    fi
}

tag_and_push () {
    tag=$1
    git tag "$tag"
    if [[ -z "$DRY_RUN" ]]; then
        git push origin tag "$tag"
    fi
}

current_release_version=$(cat VERSION)

if [[ "$CHANGE_TYPE" == "manual-override" ]]; then
    if [[ -z $RELEASE_VERSION_TO_OVERRIDE ]]; then
        echo "Manual override requires a version to override with" >&2
        exit 1
    fi

    lower_release_version=$(echo -e "$current_release_version\n$RELEASE_VERSION_TO_OVERRIDE" | sort -V | head -n 1)
    if [[ "$RELEASE_VERSION_TO_OVERRIDE" == "$lower_release_version" ]]; then
        # Delete tags for current release version
        tags_to_delete=$(git tag -l | grep "$current_release_version")
        git tag -d $tags_to_delete
        if [[ -z "$DRY_RUN" ]]; then
            git push origin --delete $tags_to_delete
        fi
    fi
    update_release_version_in_repo "Reset" "$RELEASE_VERSION_TO_OVERRIDE"
    tag_and_push "$RELEASE_VERSION_TO_OVERRIDE"
    exit 0
fi

if [[ -n $RELEASE_VERSION_TO_OVERRIDE ]]; then
    echo "Auto bump event should not take in a release version to override with" >&2
    exit 1
fi

# Auto bump event

latest_tag=$(git describe --tags --abbrev=0)
pip install parver -c .github/workflows/requirements.txt
new_tag_for_push_event="$(python3 .github/workflows/"${CHANGE_TYPE}.py" "$latest_tag")"

if [[ "$CHANGE_TYPE" == "bump-dev-num" ]]; then
    new_release_version_for_push_event=$(python3 .github/workflows/strip_prerelease_part.py "$new_tag_for_push_event")

    # Bump and commit next release version first, if we haven't already
    if [[ "$current_release_version" != "$new_release_version_for_push_event" ]]; then
        update_release_version_in_repo "Auto-bump" "$new_release_version_for_push_event"
    fi
fi

tag_and_push "$new_tag_for_push_event"
