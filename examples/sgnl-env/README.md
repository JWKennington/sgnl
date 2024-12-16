### SGNL Service Environment

The SGNSL Service Environment consists of a docker compose file running a set of
services useful for developing and testing SGNL.

## Docker background

Docker is a container runtime that works under Linux. If you are running Linux, you can run just install
and run docker-ce normally. Macs have traditionally run docker using an Linux VM. Docker offers very restrictively licensed 
desktop application called "Docker Desktop" that runs a VM and interacts with it. It requires a paid subscription.

Fortunately, there is an open source alternative, [Colima](https://github.com/abiosoft/colima). It is available via Macports (and Brew).

Once docker is available, a set of containers can be run using the `docker compose command`. The `docker compose` command 
always looks for configuration in the current directory.

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

## Run the services

Once Docker/Colima are installed and running, you can start the services using the command:
```
   docker-compose up -d
```

You should be able to see the running services using `docker ps`.

To see logs for a service, use the `docker logs` command and the container name from the docker-compose file (or `docker ps`). For example:
```
   docker logs kafka
```
To "tail" the logs for a service, use `-f`, for example: `docker logs -f kafka`.

When services are running correctly, you should see `docker ps` output like:
```
ghostwheel:~/sgn-env> docker ps
CONTAINER ID   IMAGE                              COMMAND                  CREATED          STATUS          PORTS     NAMES
957e23124b02   confluentinc/cp-kafka:latest       "/etc/confluent/dock…"   48 minutes ago   Up 48 minutes             kafka
9b6d8dfbf90e   influxdb:1.8.10                    "/entrypoint.sh -con…"   48 minutes ago   Up 48 minutes             influxdb
71960c89960e   confluentinc/cp-zookeeper:latest   "/etc/confluent/dock…"   48 minutes ago   Up 48 minutes             zookeeper
57b9b0d24d56   grafana/grafana:9.4.7              "/run.sh"                48 minutes ago   Up 48 minutes             grafana
```

The default IP address that Colima uses is `192.168.64.5`.

Kafka and influx have no authentication. Grafana has an admin user with password sgnl.


## Example commands to see that services are running:

Kafka:

```
  kcat -b 192.168.64.5:9196 -L
  kcat -b 192.168.64.5:9196 -P -t test
  kcat -b 192.168.64.5:9196 -C -t test
```

Influx:
```
  curl http://192.168.64.5:8086/health
  curl http://192.168.64.5:8086/query?pretty=true --data-urlencode "q=create database sgnl"
  curl http://192.168.64.5:8086/query?pretty=true --data-urlencode "q=show databases"  
```

Grafana:
```
  curl http://192.168.64.5/api/health
```
You should be able open a browser to `http://192.168.64.5/`. 

## Managing Colima

Colima needs to be running for the docker commands to work. To start colima:
```
    colima start --vm-type vz --network-address=true
```

Status of Colima:
```
    colima status
```
This gives basic info about the VM, including its IP address.

Stop Colima:
```
  colima stop
```



