#!/bin/bash -ex

curl -s https://packagecloud.io/install/repositories/svc-wf-jenkins/proxy-snapshot/script.deb.sh | os=any dist=any bash

find . -name "*jar" -ls