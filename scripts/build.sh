#!/bin/bash
   
# Variables
# megaWheel="macosx_10_6_intel.macosx_10_9_intel.macosx_10_9_x86_64.macosx_10_10_intel.macosx_10_10_x86_64.macosx_10_11_intel.macosx_10_11_x86_64.macosx_10_12_intel.macosx_10_12_x86_64.whl"
# baseWheelName="macosx_10_11_x86_64"
megaWheel="macosx_12_0_x86_64.whl"
baseWheelName="macosx_12_0_x86_64"
declare -a versions=("36" "37" "38" "39")
sslLibPath="/usr/local/opt/openssl@1.1/lib/"
cClientVersion=""
   
strindex() {
  x="${1%%$2*}"
  [[ "$x" = "$1" ]] && echo -1 || echo "${#x}"
}
   
clone_python_client() {
  git clone --recursive git@github.com:citrusleaf/aerospike-client-python
}
   
build_python_client() {
  export STATIC_SSL=1
  export SSL_LIB_PATH=$sslLibPath
  pushd aerospike-client-python
 
  python3 setup.py sdist --formats=gztar
 
  for version in "${versions[@]}"; do
    source ~/pythonenvs/python${version}/bin/activate
    python setup.py build --force
    python setup.py bdist_wheel
    deactivate
  done
  
  pushd dist
  echo "build_python_client ls2"
  ls -al
  for f in *.whl; do
    echo "f: $f baseWheelName: $baseWheelName"
    [[ -e $f ]] || continue
    location=`strindex $f $baseWheelName`
    fName="${f:0:$location}$megaWheel"
    mv $f $fName
  done
  popd
  popd
}
   
clone () {
  clone_python_client
}
    
build () {
  build_python_client
}
  
build_linux () {
  CLIENT_CLONE_URL="git@github.com:citrusleaf/aerospike-client-python"
  CLIENT_CLONE_REF="master"
  
  BUILDCTL="buildctl.mac"
  chmod 700 $BUILDCTL
  BUILD_NAME="manylinux2014"
  REPO=$(echo $CLIENT_CLONE_URL | cut -d':' -f2)
  BUILD=$(./$BUILDCTL rev --repo $REPO --ref $CLIENT_CLONE_REF --no-trunc | sed -n 2p | awk '{print $3}')
  URL_BASE="http://build.browser.qe.aerospike.com"
  URL="$URL_BASE/$REPO/$BUILD/build/$BUILD_NAME/build/artifacts/"
  ARTIFACT_LINKS=$(curl -s $URL | sed -n 's/.*href="\([^"]*\).*/\1/p')
  LOCAL_WHEELS="aerospike-client-python/dist"
  mkdir -p $LOCAL_WHEELS
  for ARTIFACT_LINK in $ARTIFACT_LINKS; do
          ARTIFACT_URL=$URL_BASE$ARTIFACT_LINK
          printf "$ARTIFACT_URL\n"
          if [[ $ARTIFACT_URL == *".whl" ]]; then
                  (cd $LOCAL_WHEELS && curl -O -f $ARTIFACT_URL)
          fi
  done
  ls -lat $LOCAL_WHEELS
}
  
   
###################################
# MAIN
#---------------------------------
   
clone
build
build_linux
   
printf "\nFinshed. Run 'publish.sh' to publish to production pypy or 'publish.sh -t' to publish to test pypy.\n\n"
