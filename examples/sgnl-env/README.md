### SGNL Service Environment

The SGNSL Service Environment consists of a docker compose file running a set of
services useful for developing and testing SGNL.

## Requirements 


# Macports

Brew might also work.

To install macports, see: https://www.macports.org/install.php

# Docker/Colima

Docker runs as root, but, under Mac, a virtual machine is used to run docker and all docker commands on the bas Mac operating system are run as a normal user.

To install docker under Mac:

1. Install docker
```
  port install colima docker docker-compose-plugin

```
2. Setup the docker compose plugin:
```
   mkdir -p ~/.docker/cli-plugins
   ln -s /opt/local/libexec/docker/cli-plugins/docker-compose  ~/.docker/cli-plugins   
```

3. Start colima
```
   colima start --vm-type vz --network-address=true
```

4. Test:
```
   docker ps
   docker run hello-world
```




