# --- EXAMPLE: Disable firewall (incl. after reboots)
sudo systemctl disable firewalld
sudo systemctl mask --now firewalld
sudo systemctl status firewalld
