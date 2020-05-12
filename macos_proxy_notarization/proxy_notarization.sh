set -ev

echo "Check if notarized proxy version is already in notarized dir, if it is, exit 0"
echo "===================================================================================================="
echo "Taking newest proxy and checking to see if it's already notarized:"
to_be_notarizaed="`aws s3 ls s3://eso-test-alan/to_be_notarized/ | sort -r | grep wfproxy | awk '{print $4}' | head -1 | sed 's/.tar.gz//'`"
echo $to_be_notarizaed
echo "Checking against this list that is already notarized:"
notarized="`aws s3 ls s3://eso-test-alan/notarized/ | sort -r | grep wfproxy | awk '{print $4}'`"
echo $notarized

if [[ "$notarized" == *"$to_be_notarizaed"* ]]; then
  echo "$to_be_notarizaed is in the bucket"
  exit 0
else
  echo "It's not in the directory, we need to move it into notarized bucket/do the whole process below"
fi

echo "=============STARTING======================"
ls
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

echo "Downloading Zulu JDK 11.0.7"
ZULU_JDK="`wget https://cdn.azul.com/zulu/bin/zulu11.39.15-ca-jdk11.0.7-macosx_x64.tar.gz`"
$ZULU_JDK
tar xvzf zulu11.39.15-ca-jdk11.0.7-macosx_x64.tar.gz

echo "Codesigning & timestamping each file in the JDK/JRE"
find "zulu11.39.15-ca-jdk11.0.7-macosx_x64/zulu-11.jdk" -type f \( -name "*.jar" -or -name "*.dylib" -or -perm +111 -type f -or -type l \) -exec codesign -f -s "$WF_DEV_ACCOUNT" --entitlements "./macos_proxy_notarization/wfproxy.entitlements" {} --timestamp --options runtime \;

echo "Downloading previous proxy release"
TO_BE_NOTARIZED=$(aws s3 ls s3://eso-test-alan/to_be_notarized/ | sort -r | grep wfproxy | head -1 | awk '{print $4}')
echo $TO_BE_NOTARIZED

echo "Get the version"
RE=[0-9]+\.[0-9]+\.[0-9]+
if [[ $TO_BE_NOTARIZED =~ $RE ]]; then 
  echo ${BASH_REMATCH[0]};
  VERSION=${BASH_REMATCH[0]} 
fi
echo $VERSION

copy_from_to_be_notarized="aws s3 cp s3://eso-test-alan/to_be_notarized/wfproxy-$VERSION.tar.gz ."
$copy_from_to_be_notarized
tarfile="wfproxy-$VERSION.tar.gz"
tar xvzf $tarfile

rm -rf lib/jdk
mkdir lib/jdk
cp -r zulu11.39.15-ca-jdk11.0.7-macosx_x64/zulu-11.jdk/Contents/Home/* lib/jdk/;

zip -r wfproxy-$VERSION.zip bin/ etc/ lib/

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

sleep 60

echo "Executing this command to see the status of notarization"
xcrun altool --notarization-info "$requestuuid" -u "$USERNAME" -p "$APP_SPECIFIC_PW"

status="$(xcrun altool --notarization-info "$requestuuid" -u "$USERNAME" -p "$APP_SPECIFIC_PW")"
in_progress='Status: in progress'
success='Status Message: Package Approved'
invalid='Status: invalid'

while true;
do
  echo $status
  if [[ "$status" == *"$success"* ]]; then
    echo "Successful notarization"
    aws s3 cp wfproxy-$VERSION.zip s3://eso-test-alan/notarized/
    exit 0
  elif [[ "$status" == *"$in_progress"* ]]; then
    status="$(xcrun altool --notarization-info "$requestuuid" -u "$USERNAME" -p "$APP_SPECIFIC_PW")"
    sleep 60
  elif [[ "$status" == *"$invalid"* ]]; then
    echo "Failed notarization"
    exit 1
  fi
done
