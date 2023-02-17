#!/bin/bash

set -x

CLIENT_CLONE_URL=$1
CLIENT_CLONE_REV=$3
BUILD_NAME=$2
REPO=$(echo $CLIENT_CLONE_URL | cut -d':' -f2)

BUILDCTL_DOWNLOAD="http://build.browser.qe.aerospike.com/citrusleaf/qe.go/3.3.0-52-gd66bd4b/build/1.17/default/artifacts/buildctl.linux"
BUILDCTL="buildctl.linux"

if [ ! -f $BUILDCTL ]; then
	curl -O -f $BUILDCTL_DOWNLOAD
	chmod 700 $BUILDCTL
fi

BUILD=$(./$BUILDCTL rev --repo $REPO --ref $CLIENT_CLONE_REV | sed -n 2p | awk '{print $3}')

URL_BASE="http://build.browser.qe.aerospike.com"
URL="$URL_BASE/$REPO/$BUILD/build/$BUILD_NAME/community/artifacts"

# Download the artifact
wget -r --no-parent --accept "*.deb" --no-directories $URL
