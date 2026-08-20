#!/bin/bash
# Starts munged (auth), fixes up the shared boundary-log volume's
# permissions (must be writable by every hook identity: root,
# SlurmUser, and the submitting user), then execs the
# requested daemon in the foreground so Docker can supervise it.
set -euo pipefail

mkdir -p /var/log/ang/boundary
chmod 777 /var/log/ang/boundary

chown munge:munge /etc/munge/munge.key
chmod 400 /etc/munge/munge.key
mkdir -p /run/munge
chown munge:munge /run/munge
runuser -u munge -- /usr/sbin/munged

case "${1:-}" in
  slurmctld)
    exec /usr/sbin/slurmctld -D
    ;;
  slurmd)
    exec /usr/sbin/slurmd -D
    ;;
  *)
    echo "usage: entrypoint.sh <slurmctld|slurmd>" >&2
    exit 2
    ;;
esac
