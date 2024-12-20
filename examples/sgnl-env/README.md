# SGNL Service Environment

The SGNSL Service Environment consists of a docker compose file running a set of
services useful for developing and testing SGNL. 

The containers that are run are by docker compose are:

1. [kafka](https://hub.docker.com/r/confluentinc/cp-kafka)
2. [zookeeper](https://hub.docker.com/r/confluentinc/cp-zookeeper)
3. [influxdb](https://hub.docker.com/_/influxdb)
4. [grafana](https://hub.docker.com/r/grafana/grafana)
5. [sgnl-services](https://git.ligo.org/greg/sgnl-microservices)

These are all standard containers exept [SGNL Microservices](https://git.ligo.org/greg/sgnl-microservices) 
which is homegrown and runs a web server that provides some HTTP services that sgnl expects in production.

## Docker background

Docker is a container runtime that works under Linux. If you are running Linux, you can run just install
and run docker-ce normally. Macs have traditionally run docker using an Linux VM. Docker offers very restrictively licensed 
desktop application called "Docker Desktop" that runs a VM and interacts with it. It requires a paid subscription.

Fortunately, there is an open source alternative, [Colima](https://github.com/abiosoft/colima). It is available via Macports (and Brew).

Once docker is available, a set of containers can be run using the `docker compose command`. The `docker compose` command 
always looks for configuration in the current directory.

URLs for services depend on where you are using them. Outside of containers, you just use `localhost-v4:PORT` (`127.0.0.1:PORT`). 
The more familiar `localhost:PORT` works most of the time, but can fail because Mac uses `localhost` for both an IPv4 address and
an IPv6 address. In containers you need to refer to services on other containers using `CONTAINER_NAME:PORT` (e.g., `influxdb:8086`). This is mostly an
issue when configuring data source URLs in grafana.

## Requirements 

### Macports

Brew might also work.

To install macports, see: https://www.macports.org/install.php

### Docker/Colima

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

### Condor

The documentation for setting up condor locally is [here](https://htcondor.readthedocs.io/en/latest/getting-htcondor/install-linux-as-user.html).
The quick start is:
```
  curl -fsSL https://get.htcondor.org | /bin/bash -s -- --download
  tar xvzf condor.tar.gz
  mv condor-*stripped condor
  cd condor
  ./bin/make-personal-from-tarball
```

You will now have a condor installation. Every time that you login (maybe setup your .bash_profile), run:
```
  . ~/condor/condor.sh
```

To start HTCondor:
```
  condor_master
```

Test that things are working:
```
  condor_status
  condor_q
```

Try submitting a job. In the sgnl-env directory:
```
      cd condor
      condor_submit test.sub
      condor_q
      condor_status
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
957e23124b02   confluentinc/cp-kafka:latest                       "/etc/confluent/dock…"   48 minutes ago   Up 48 minutes             kafka
9b6d8dfbf90e   influxdb:1.8.10                                    "/entrypoint.sh -con…"   48 minutes ago   Up 48 minutes             influxdb
71960c89960e   confluentinc/cp-zookeeper:latest                   "/etc/confluent/dock…"   48 minutes ago   Up 48 minutes             zookeeper
57b9b0d24d56   grafana/grafana:9.4.7                              "/run.sh"                48 minutes ago   Up 48 minutes             grafana
048e0deb9b6a   containers.ligo.org/greg/sgnl-microservices:0.0.2  "python3 /services/m?"   48 minutes ago   Up 48 minutes             sgnl-services
```

Kafka and influx have no authentication. Grafana has an admin user with password sgnl.

## Example commands to see that services are running:

Kafka:

```
  kcat -b localhost-v4:9196 -L
  kcat -b localhost-v4:9196 -P -t test
  kcat -b localhost-v4:9196 -C -t test
```

If `localhost-v4` doesn't work, try `make hostalias` or use `127.0.0.1` instead of `localhost-v4`. 

InfluxDB:
```
  curl http://localhost:8086/health
  curl http://localhost:8086/query?pretty=true --data-urlencode "q=create database sgnl"
  curl http://localhost:8086/query?pretty=true --data-urlencode "q=show databases"  
```

Grafana:
```
  curl http://localhost/api/health
```
You should be able open a browser to `http://localhost/`. 

SGNL Services:
```
    curl -D -   http://localhost:5000/

    curl -D -   http://localhost:5000/cgi-bin/interval -H  'Content-Type: application/json' -d'
{
  "target":"{\"from\": 1660076939153, \"to\": 1660081939153}"
}'
```

Important note: the script `microservices.py` is bind mounted in the `sgnl-services` container for
speedy development. If you need to change the behavior of the mock gracedb server or the 
microservices you can update this version and restart the `sgnl-services` container. 
Eventually, the changes should be propagated to the
version in the `sgnl-microservices` repository.


## Manage the services

You can restart a single service/container by container name, for example:
```
    docker compose restart sgnl-services
```

You can restart all services using the command:
```
    docker compose restart
```

You can stop all services using the command:
```
   docker compose down
```


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

## Configure Services

Kafka does not require any configuration.

You will need to create an InfluxDB database for each analysis. There is an example above
that shows how to create a database using curl. You can use:
```
    make influxdb_sgnl
```
to create an InfluxDB database named `sgnl`.

The typical analysis dashboard requires some helper datasources (the same for all analyses). You can create them and a datasource that points at the InfluxDB database named `sgnl` using make:
```
  make datasources
```


## How it all fits together

SGNL jobs run on your local box using condor (or just started on the command line). The SGNL jobs
use kafka for communication (`localhost:9196`), send metrics to influxdb (`localhost:8086`),
and read/write _mock GraceDB_ (`localhost:5000`).

You can monitor the sgnl jobs using dashboards on grafana (`http://localhost`). 
Grafana uses influxdb (`influxdb:8086`) and sgnl-services (`sgnl-services:5000`) for data sources.




