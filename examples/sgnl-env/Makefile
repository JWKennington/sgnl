dirs:
	mkdir -p grafana_data influx_data kafka_data zoo_data zoo_logs

install_docker:
	port install colima docker docker-compose-plugin

start_docker:
	   colima start --vm-type vz --network-address=true

stop_docker:
	colima stop

start_services:
	docker compose up -d

stop_services:
	docker compose down
