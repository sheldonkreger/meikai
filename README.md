# Blockchain Data Platform for Intelligence and Investigations

# Meikai
Blockchain Forensics Toolkit

## Initialize Development Environment
This team uses Fedora Linux.

### Redash
https://github.com/getredash/redash/wiki/Local-development-setup

### Neo4J
https://neo4j.com/download-thanks-desktop/?edition=desktop&flavour=linux&release=1.5.9&offline=true#installation-guide

### ClickHouse
Docker installation is recommended. Be sure it include the arguments for networking between localhost and the Docker container.
https://hub.docker.com/r/clickhouse/clickhouse-server/#networking
```
docker run -d --network=host --name meikai --ulimit nofile=262144:262144 clickhouse/clickhouse-server
echo 'SELECT version()' | curl 'http://localhost:8123/' --data-binary @-
```

### Ansible
Installation via pipx is recommended because it will isolate the ansible package without impacting your global Python3 installation.
https://docs.ansible.com/ansible/latest/installation_guide/intro_installation.html#installing-and-upgrading-ansible-with-pipx
