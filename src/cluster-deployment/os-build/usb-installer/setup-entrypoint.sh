# Drops console into entrypoint script on boot
if [ -f /entrypoint.sh ]; then
  chmod 0755 /entrypoint.sh
fi
sed -i -E 's#^tty1.*#tty1::respawn:/sbin/getty -n -l /entrypoint.sh 38400 tty1 linux#' /etc/inittab
