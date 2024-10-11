#!/usr/bin/env bash

user='pqvfumqbqp'
password='ioweb123GUIweb'

apt install samba
cat <<EOF | sudo tee /etc/samba/smb.conf
[global]
usershare allow guests = no
# Does not work. Somehow hangs indefinitely during systemctl restart smbd nmbd
#bind interfaces only = yes
#interfaces = lo

[test-share]
path = /tmp/smbshare
browsable = yes
guest ok = no
read only = yes
create mask = 0755
EOF

# Unfortunately, we need a user because anonymous/guest login does not seem to work with smbprotocol:
# https://github.com/jborean93/smbprotocol/issues/168
adduser --quiet --no-create-home --disabled-password --disabled-login "$user"
smbpasswd -a "$user" -w "$password"  # type password here ioweb123GUIweb

systemctl restart smbd nmbd
