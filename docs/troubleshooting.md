# Troubleshooting
This guide provides solutions to common issues encountered while working with the project.

## Linux OOM Killer
If your terminal session terminates unexpectedly while running resource-intensive 
SQLMesh operations, it may be due to the Linux Out-Of-Memory (OOM) killer intervening 
when memory usage exceeds limits.

You can prevent this by disabling the `systemd-oomd` service:
```bash
sudo systemctl disable --now systemd-oomd.service
sudo systemctl disable --now systemd-oomd.socket
```

Verify that the services are disabled:
```bash
systemctl status systemd-oomd.service
systemctl status systemd-oomd.socket
```