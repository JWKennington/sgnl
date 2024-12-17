start: start_docker start_services
	sgnl-mock-gracedb-server

view:
	sleep 2
	open http://localhost
	sleep 2
	open http://127.0.0.1:5000/

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
