start: start_docker start_services
	echo "starting docker and services"

stop: stop_docker stop_services
	echo "stopping docker and services"

install_docker: grafana_data influx_data kafka_data zoo_data zoo_logs
	port install colima docker docker-compose-plugin

grafana_data:
	mkdir -p $@
influx_data:
	mkdir -p $@
kafka_data:
	mkdir -p $@
zoo_data zoo_logs:
	mkdir -p $@

start_docker:
	   colima start --vm-type vz --network-address=true

stop_docker:
	colima stop

start_services:
	docker compose up -d

stop_services:
	docker compose down
