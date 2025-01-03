all: inspiral range scald event_uploader event_plotter

H1_SVD_BANK=H1-0000_GSTLAL_SVD_BANK-1241725020-760106.xml.gz
L1_SVD_BANK=L1-0000_GSTLAL_SVD_BANK-1241725020-760106.xml.gz
V1_SVD_BANK=V1-0000_GSTLAL_SVD_BANK-1241725020-760106.xml.gz
EVENT_CONFIG=../../config/fake_cbc.yaml

KAFKA_SERVER=localhost-v4:9196
GRACEDB_GROUP=CBC
GRACEDB_PIPELINE=SGN
GRACEDB_SEARCH=MOCK
GRACEDB_LABEL=MOCK
GRACEDB_URL=http://127.0.0.1:5000/api/
TAG=sgnl_test
JOB_TAG=0000_noninj
FAR_THRESH=1e-6

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
		--event-config ${EVENT_CONFIG} \
		--trigger-output out.sqlite \
		--output-likelihood-file likelihood_ratio.xml.gz \
		--output-kafka-server ${KAFKA_SERVER} \
		--gracedb-far-threshold ${FAR_THRESH} \
		--analysis-tag ${TAG} \
		--job-tag ${JOB_TAG}

inspiral_gracedb:
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
		--event-config ${EVENT_CONFIG} \
		--trigger-output out.sqlite \
		--output-likelihood-file likelihood_ratio.xml.gz \
		--gracedb-far-threshold ${FAR_THRESH} \
		--gracedb-group ${GRACEDB_GROUP} \
		--gracedb-pipeline ${GRACEDB_PIPELINE} \
		--gracedb-search ${GRACEDB_SEARCH} \
		--gracedb-label ${GRACEDB_LABEL} \
		--gracedb-service-url ${GRACEDB_URL} \

scald: scald_single scald_across_jobs

scald_single:
	scald aggregate \
		--config inspiral.yml \
		--uri kafka://${TAG}-collect@${KAFKA_SERVER} \
		--data-type timeseries \
		--topic sgnl.${TAG}.latency_history_max \
		--topic sgnl.${TAG}.latency_history_median \
		--topic sgnl.${TAG}.H1_datasource_latency \
		--topic sgnl.${TAG}.L1_datasource_latency \
		--topic sgnl.${TAG}.V1_datasource_latency \
		--topic sgnl.${TAG}.H1_whitening_latency \
		--topic sgnl.${TAG}.L1_whitening_latency \
		--topic sgnl.${TAG}.V1_whitening_latency \
		--topic sgnl.${TAG}.H1_snrSlice_latency \
		--topic sgnl.${TAG}.L1_snrSlice_latency \
		--topic sgnl.${TAG}.V1_snrSlice_latency \
		--topic sgnl.${TAG}.all_itacacac_latency \
		--topic sgnl.${TAG}.H1_snr_history \
		--topic sgnl.${TAG}.L1_snr_history \
		--topic sgnl.${TAG}.V1_snr_history \
		--topic sgnl.${TAG}.range_history \
		--topic sgnl.${TAG}.event_uploader_heartbeat \
		--topic sgnl.${TAG}.event_plotter_heartbeat \
		--schema latency_history_max \
		--schema latency_history_median \
		--schema H1_datasource_latency \
		--schema L1_datasource_latency \
		--schema V1_datasource_latency \
		--schema H1_whitening_latency \
		--schema L1_whitening_latency \
		--schema V1_whitening_latency \
		--schema H1_snrSlice_latency \
		--schema L1_snrSlice_latency \
		--schema V1_snrSlice_latency \
		--schema all_itacacac_latency \
		--schema H1_snr_history \
		--schema L1_snr_history \
		--schema V1_snr_history \
		--schema range_history \
		--schema event_uploader_heartbeat \
		--schema event_plotter_heartbeat

scald_across_jobs:
	scald aggregate \
		--config inspiral.yml \
		--uri kafka://${TAG}-collect@${KAFKA_SERVER} \
		--data-type timeseries \
		--topic sgnl.${TAG}.likelihood_history \
		--topic sgnl.${TAG}.far_history \
		--topic sgnl.${TAG}.ram_history \
		--schema likelihood_history \
		--schema far_history \
		--schema ram_history \
		--across-jobs



range: h1_range l1_range v1_range

h1_range:
	sgnl-ll-dq \
		--data-source white-realtime \
		--psd-fft-length 4 \
		--input-sample-rate 16384 \
		--channel-name H1=FAKE \
		--scald-config inspiral.yml \
		--output-kafka-server ${KAFKA_SERVER} \
		--analysis-tag ${TAG}

l1_range:
	sgnl-ll-dq \
		--data-source white-realtime \
		--psd-fft-length 4 \
		--input-sample-rate 16384 \
		--channel-name L1=FAKE \
		--scald-config inspiral.yml \
		--output-kafka-server ${KAFKA_SERVER} \
		--analysis-tag ${TAG}

v1_range:
	sgnl-ll-dq \
		--data-source white-realtime \
		--psd-fft-length 4 \
		--input-sample-rate 16384 \
		--channel-name V1=FAKE \
		--scald-config inspiral.yml \
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
		--gracedb-service-url ${GRACEDB_URL} \

event_plotter: plot_ranking_data plot_snr_plots plot_psd_plots plot_dtdphi_plots #plot_ranking_plot 

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

clean:
	rm -rf out.sqlite likelihood_ratio.xml.gz
