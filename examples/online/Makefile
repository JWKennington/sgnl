all: inspiral scald #event_uploader event_plotter

H1_SVD_BANK=/ligo/home/ligo.org/yun-jing.huang/phd/benchmark/gstlal-benchmarking/run23_o4banks/H1-0750_GSTLAL_SVD_BANK-0-0.xml.gz
L1_SVD_BANK=/ligo/home/ligo.org/yun-jing.huang/phd/benchmark/gstlal-benchmarking/run23_o4banks/L1-0750_GSTLAL_SVD_BANK-0-0.xml.gz
V1_SVD_BANK=/ligo/home/ligo.org/yun-jing.huang/phd/benchmark/gstlal-benchmarking/run23_o4banks/V1-0750_GSTLAL_SVD_BANK-0-0.xml.gz

KAFKA_SERVER= # FIXME
GRACEDB_GROUP=CBC
GRACEDB_PIPELINE=SGN
GRACEDB_SEARCH=MOCK
GRACEDB_URL= # FIXME
TAG=sgnl_test

inspiral:
	sgnl-inspiral \
		--data-source white-realtime \
		--psd-fft-length 4 \
        --input-sample-rate 16384 \
		--channel-name H1=FAKE \
		--channel-name L1=FAKE \
		--channel-name V1=FAKE \
		--torch-device cpu --torch-dtype float32 --trigger-finding-duration 1 \
		--svd-bank ${H1_SVD_BANK} \
		--svd-bank ${L1_SVD_BANK} \
		--svd-bank ${V1_SVD_BANK} \
		--event-config fake_cbc.yaml \
		--trigger-output out.sqlite \
		--output-likelihood-file likelihood_ratio.xml.gz \
		--output-kafka-server ${KAFKA_SERVER} \
		--analysis-tag ${TAG}

event_uploader:
	sgnl-ll-inspiral-event-uploader \
		--kafka-server ${KAFKA_SERVER} \
		--gracedb-group ${GRACEDB_GROUP} \
		--gracedb-pipeline ${GRACEDB_PIPELINE} \
		--gracedb-search ${GRACEDB_SEARCH} \
		--far-threshold 3.8e-07 \
		--far-trials-factor 6 \
		--upload-cadence-type geometric \
		--upload-cadence-factor 2 \
		--num-jobs 1 \
		--tag ${TAG} \
		--input-topic events \
		--verbose \
		--scald-config inspiral.yml \
		--max-partitions 10 \
		--gracedb-service-url GRACEDB_URL \

event_plotter: plot_ranking_data plot_ranking_plot plot_snr_plots plot_psd_plots plot_dtdphi_plots

plot_ranking_data:
	sgnl-ll-inspiral-event-plotter \
		--kafka-server ${KAFKA_SERVER} \
		--upload-topic uploads \
		--ranking-stat-topic ranking_stat \
		--tag ${TAG} \
		--plot RANKING_DATA \
		--verbose \
		--gracedb-service-url ${GRACEDB_URL}

plot_ranking_plot:
	sgnl-ll-inspiral-event-plotter \
		--kafka-server ${KAFKA_SERVER} \
		--upload-topic uploads \
		--ranking-stat-topic ranking_stat \
		--tag ${TAG} \
		--plot RANKING_PLOTS \
		--verbose \
		--gracedb-service-url ${GRACEDB_URL}

plot_snr_plots:
	sgnl-ll-inspiral-event-plotter \
		--kafka-server ${KAFKA_SERVER} \
		--upload-topic uploads \
		--ranking-stat-topic ranking_stat \
		--tag ${TAG} \
		--plot SNR_PLOTS \
		--verbose \
		--gracedb-service-url ${GRACEDB_URL}

plot_psd_plots:
	sgnl-ll-inspiral-event-plotter \
		--kafka-server ${KAFKA_SERVER} \
		--upload-topic uploads \
		--ranking-stat-topic ranking_stat \
		--tag ${TAG} \
		--plot PSD_PLOTS \
		--verbose \
		--gracedb-service-url ${GRACEDB_URL}

plot_dtdphi_plots:
	sgnl-ll-inspiral-event-plotter \
		--kafka-server ${KAFKA_SERVER} \
		--upload-topic uploads \
		--ranking-stat-topic ranking_stat \
		--tag ${TAG} \
		--plot DTDPHI_PLOTS \
		--verbose \
		--gracedb-service-url ${GRACEDB_URL}

scald: scald_latency

scald_latency:
	scald aggregate \
		--config inspiral.yml \
		--uri kafka://${TAG}-collect@${KAFKA_SERVER} \
		--data-type timeseries \
		--topic sgnl.${TAG}.latency_history_max \
		--schema latency_history_max \
