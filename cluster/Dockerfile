# Slurm cluster image for the 8-node capture cluster: stock slurm-wlm plus the
# ANG boundary hooks and a non-root submitting user. slurm.conf hardcodes the
# node count, so a different cluster size means a different slurm.conf.
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y --no-install-recommends slurm-wlm munge && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /var/spool/slurmctld /var/spool/slurmd /var/log/slurm /etc/slurm/ang-hooks && \
    chown slurm:slurm /var/spool/slurmctld /var/log/slurm && \
    useradd -m -u 2000 angsub

COPY slurm.conf /etc/slurm/slurm.conf
COPY ang-hooks/ /etc/slurm/ang-hooks/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh /etc/slurm/ang-hooks/*.sh

ENTRYPOINT ["/entrypoint.sh"]
