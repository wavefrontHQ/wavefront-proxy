set -ev

WFPROXY_ZIP_TO_BE_NOTARIZED=$1
echo "This is the zip that was just uplaoded: $1"

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

  echo "Delete certs"
  rm -fr *.p12
}

# Parse the proxy version our of the 
parse_proxy_version_from_zip() {
  echo "Get the version"
  TO_BE_NOTARIZED=$(aws s3 ls s3://eso-wfproxy-testing/to_be_notarized/$WFPROX_ZIP_TO_BE_NOTARIZED | awk '{print $4}')
  RE=[0-9]+\.[0-9]+.[0-9-_]+
  if [[ $TO_BE_NOTARIZED =~ $RE ]]; then 
    echo ${BASH_REMATCH[0]};
    VERSION=${BASH_REMATCH[0]} 
  fi
  echo $VERSION
}

# Notarized the .zip and upload to Apply
notarized_newly_package_proxy() {
  echo "Downloading the ZIP to be notarized"
  aws s3 cp s3://eso-wfproxy-testing/to_be_notarized/$WFPROXY_ZIP_TO_BE_NOTARIZED .
  echo "Codesigning the wavefront-proxy package"
  codesign -f -s "$ESO_DEV_ACCOUNT" $WFPROXY_ZIP_TO_BE_NOTARIZED --deep --options runtime

  echo "Verifying the codesign"
  codesign -vvv --deep --strict $WFPROXY_ZIP_TO_BE_NOTARIZED

  echo "Uploading the package for Notarization"
  response="$(xcrun altool --notarize-app --primary-bundle-id "com.wavefront" --username "$USERNAME" --password "$APP_SPECIFIC_PW" --file "$WFPROXY_ZIP_TO_BE_NOTARIZED" | sed -n '2 p')"
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
      aws s3 cp $WFPROXY_ZIP_TO_BE_NOTARIZED s3://eso-wfproxy-testing/notarized_test/wavefront-proxy-notarized-$VERSION.zip
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
  create_dev_certs
  parse_proxy_version_from_zip
  echo $VERSION
  notarized_newly_package_proxy
  sleep 20
  wait_for_notarization
}

main