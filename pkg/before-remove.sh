#!/bin/bash -e

service_name="wavefront-proxy"
wavefront_dir="/opt/wavefront"
jre_dir="$wavefront_dir/$service_name/proxy-jre"

#service wavefront-proxy stop || true

# rpm passes "0", "1" or "2" as a command line argument - due to the order in which scripts are executed by rpm
# (post-install for new version first and only then before-remove for the old version), we can only stop the
# service and safely delete the JRE that we downloaded if the argument is "0", meaning that it's an uninstall
#
# Ref:
#  - http://www.rpm.org/max-rpm/s1-rpm-inside-scripts.html
#  - https://www.debian.org/doc/debian-policy/ch-maintainerscripts.html
if [[ "$1" == "0" ]] || [[ "$1" == "remove" ]] || [[ "$1" == "purge" ]]; then
    service wavefront-proxy stop || true
    if [ -d $jre_dir ]; then
      [ "$(ls -A $jre_dir)" ] && echo "Removing installed JRE from $jre_dir" >&2
      rm -rf $jre_dir
    fi
fi

exit 0
