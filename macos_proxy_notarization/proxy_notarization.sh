set -ev

WFPROXY_TARBALL=$1
echo "This is the tarball that was just uplaoded: $1"
PARSED_TARBALL="`echo $WFPROXY_TARBALL| sed 's/.tar.gz//'`"
echo $PARSED_TARBALL

echo "List of proxy that are already notarized:"
LIST_ALREADY_NOTARIZED="`aws s3 ls s3://eso-wfproxy-testing/brew/ | sort -r | grep wfproxy | awk '{print $4}'`"
echo $LIST_ALREADY_NOTARIZED

# Checking against this list that is already notarized
check_notarized_list() {
  if [[ "$LIST_ALREADY_NOTARIZED" == *"$PARSED_TARBALL"* ]]; then
    echo "$PARSED_TARBALL is in the bucket"
    exit 0
  else
    echo "It's not in the directory, we need to do the whole notarization process and move it into brew folder."
  fi
}
# Create Apple Developer certs on travisci env
create_dev_certs() {
  echo "Adding OSX Certificates"
  KEY_CHAIN=build.keychain
  CERTIFICATE_P12=certificate.p12
  ESO_TEAM_P12=eso_certificate.p12

  echo "Recreate the certificate from the secure environment variable"
  echo $WAVEFRONT_TEAM_CERT_P12 | base64 -D -o  $ESO_TEAM_P12;
  echo $CERTIFICATE_OSX_P12 | base64 -D -o  $CERTIFICATE_P12;

  echo "Create a keychain"
  security create-keychain -p travis $KEY_CHAIN

  echo "Make the keychain the default so identities are found"
  security default-keychain -s $KEY_CHAIN

  echo "Unlock the keychain 1"
  security unlock-keychain -p travis $KEY_CHAIN

  echo "Unlock the keychain 2"
  ls
  echo $CERTIFICATE_P12
  echo $ESO_TEAM_P12
  security import ./eso_certificate.p12 -x -t agg -k $KEY_CHAIN -P $WAVEFRONT_TEAM_CERT_PASSWORD -T /usr/bin/codesign;
  security import ./certificate.p12 -x -t agg -k $KEY_CHAIN -P $CERTIFICATE_PASSWORD -T /usr/bin/codesign;

  echo "Finding identity"
  security find-identity -v

  echo "Unlock the keychain 3"
  security set-key-partition-list -S apple-tool:,apple:,codesign: -s -k travis $KEY_CHAIN

  echo "Remove certs"
  rm -fr *.p12
}

# Download the signed JDK from AWS S3 Bucket
download_jdk() {
  echo "Downloading Zulu JDK 11.0.7"
  aws s3 cp s3://wavefront-misc/PROXY-JRE-IS-ZULU-11.0.7/zulu11.39.15-ca-jdk11.0.7-macosx_x64.zip .
  unzip zulu11.39.15-ca-jdk11.0.7-macosx_x64.zip
}

# NOT NEEDED?
# Download non-notarized wf-proxy that was uploaded to 'to_be_notarized' dir, via jenkins job 
download_proxy_non_notarized() {
  echo "Downloading recently uploaded proxy from to_be_notarized:"
  # TO_BE_NOTARIZED=$(aws s3 ls s3://eso-wfproxy-testing/to_be_notarized/ | sort -r | grep wfproxy | head -1 | awk '{print $4}')
  # echo $TO_BE_NOTARIZED
  TO_BE_NOTARIZED=$(aws s3 ls s3://eso-wfproxy-testing/to_be_notarized/$WFPROXY_TARBALL | awk '{print $4}')
  echo $TO_BE_NOTARIZED
}

# Parse the proxy version our of the 
parse_proxy_version_tarball() {
  echo "Get the version"
  TO_BE_NOTARIZED=$(aws s3 ls s3://eso-wfproxy-testing/to_be_notarized/$WFPROXY_TARBALL | awk '{print $4}')
  RE=[0-9]+\.[0-9]+\.[0-9]+
  if [[ $TO_BE_NOTARIZED =~ $RE ]]; then 
    echo ${BASH_REMATCH[0]};
    VERSION=${BASH_REMATCH[0]} 
  fi
  echo $VERSION
}

# CP non-notarized proxy, cp signed jdk with it, package as .zip
repackage_proxy() {
  echo $VERSION
  COPY_FORM_TO_BE_NOTARIZED="aws s3 cp s3://eso-wfproxy-testing/to_be_notarized/wfproxy-$VERSION.tar.gz ."
  $COPY_FORM_TO_BE_NOTARIZED
  TARBALL="wfproxy-$VERSION.tar.gz"
  tar xvzf $TARBALL
  rm -rf lib/jdk
  mkdir lib/jdk
  ls
  cp -r zulu11.39.15-ca-jdk11.0.7-macosx_x64/zulu-11.jdk/Contents/Home/* lib/jdk/;
  zip -r wfproxy-$VERSION.zip bin/ etc/ lib/
}

# Notarized the .zip and upload to Apply
notarized_newly_package_proxy() {
  echo "Codesigning the wavefront-proxy package"
  codesign -f -s "$ESO_DEV_ACCOUNT" wfproxy-$VERSION.zip --deep --options runtime

  echo "Verifying the codesign"
  codesign -vvv --deep --strict wfproxy-$VERSION.zip

  echo "Uploading the package for Notarization"
  response="$(xcrun altool --notarize-app --primary-bundle-id "com.wavefront" --username "$USERNAME" --password "$APP_SPECIFIC_PW" --file "wfproxy-$VERSION.zip" | sed -n '2 p')"
  echo $response

  echo "Grabbing Request UUID"
  requestuuid=${response#*= }
  echo $requestuuid

  echo "Executing this command to see the status of notarization"
  xcrun altool --notarization-info "$requestuuid" -u "$USERNAME" -p "$APP_SPECIFIC_PW"
}

# Pass or fail based on notarization status
wait_for_notarization() {
  status="$(xcrun altool --notarization-info "$requestuuid" -u "$USERNAME" -p "$APP_SPECIFIC_PW")"
  in_progress='Status: in progress'
  success='Status Message: Package Approved'
  invalid='Status: invalid'

  while true;
  do
    echo $status
    if [[ "$status" == *"$success"* ]]; then
      echo "Successful notarization"
      aws s3 cp wfproxy-$VERSION.zip s3://eso-wfproxy-testing/brew/
      exit 0
    elif [[ "$status" == *"$in_progress"* ]]; then
      status="$(xcrun altool --notarization-info "$requestuuid" -u "$USERNAME" -p "$APP_SPECIFIC_PW")"
      sleep 60
    elif [[ "$status" == *"$invalid"* ]]; then
      echo "Failed notarization"
      exit 1
    fi
  done
}

main() {
  check_notarized_list
  create_dev_certs
  download_jdk
  parse_proxy_version_tarball
  echo $VERSION
  repackage_proxy
  notarized_newly_package_proxy
  sleep 20
  wait_for_notarization
}

main