all : H1L1V1-SGNL_INSPIRAL_C2_FAR-1241738590-3835.sqlite.gz missed_found.png

# Data are in /home/yun-jing.huang/phd/sgn/run/offline on CIT
H1L1V1-SGNL_INSPIRAL-1241738590-3835.sqlite.gz :
	sgnl-inspiral \
		--ht-gate-threshold 50.0 \
		--gps-start-time 1241738590 \
		--gps-end-time 1241742425 \
		--channel-name H1=GWOSC-16KHZ_R1_STRAIN \
		--channel-name L1=GWOSC-16KHZ_R1_STRAIN \
		--channel-name V1=GWOSC-16KHZ_R1_STRAIN \
		--frame-cache frame.cache \
		--track-psd \
		--data-source frames \
		--psd-fft-length 8 \
		--frame-segments-name datasegments \
		--coincidence-threshold 0.01 \
		--svd-bank H1-0000_GSTLAL_SVD_BANK-1241725020-760106.xml.gz \
		--svd-bank L1-0000_GSTLAL_SVD_BANK-1241725020-760106.xml.gz \
		--svd-bank V1-0000_GSTLAL_SVD_BANK-1241725020-760106.xml.gz \
		--reference-psd H1L1V1-GSTLAL_REFERENCE_PSD-1241738590-3835.xml.gz \
		--frame-segments-file segments.xml.gz \
		--trigger-output $@ \
		--torch-dtype float32 \
		--trigger-finding-duration 1 \
		--event-config fake_cbc.yaml \
		--ranking-stat-output H1L1V1-SGNL_INSPIRAL_STATS-1241738590-3835.xml.gz

# Inj data are in /home/yun-jing.huang/phd/sgn/run/offline-injections on CIT
H1L1V1-SGNL_INSPIRAL_INJ-1241738590-3835.sqlite.gz :
	sgnl-inspiral \
		--ht-gate-threshold 50.0 \
		--gps-start-time 1241738590 \
		--gps-end-time 1241742425 \
		--channel-name H1=GWOSC-16KHZ_R1_STRAIN \
		--channel-name L1=GWOSC-16KHZ_R1_STRAIN \
		--channel-name V1=GWOSC-16KHZ_R1_STRAIN \
		--frame-cache frame.cache \
		--track-psd \
		--data-source frames \
		--psd-fft-length 8 \
		--frame-segments-name datasegments \
		--coincidence-threshold 0.01 \
		--svd-bank H1-0000_GSTLAL_SVD_BANK-1241725020-760106.xml.gz \
		--svd-bank L1-0000_GSTLAL_SVD_BANK-1241725020-760106.xml.gz \
		--svd-bank V1-0000_GSTLAL_SVD_BANK-1241725020-760106.xml.gz \
		--reference-psd H1L1V1-GSTLAL_REFERENCE_PSD-1241738590-3835.xml.gz \
		--frame-segments-file segments.xml.gz \
		--trigger-output $@ \
		--torch-dtype float32 \
		--trigger-finding-duration 1 \
		--event-config fake_cbc.yaml \
		--injections \
		--injection-file injections.xml.gz \
		--noiseless-inj-frame-cache injection-frame.cache \
		--noiseless-inj-channel-name H1=RPO4_OFFLINE_v1 \
		--noiseless-inj-channel-name L1=RPO4_OFFLINE_v1 \
		--noiseless-inj-channel-name V1=RPO4_OFFLINE_v1

H1L1V1-SGNL_INSPIRAL_C1-1241738590-3835.sqlite.gz : H1L1V1-SGNL_INSPIRAL-1241738590-3835.sqlite.gz
	stillsuit-merge-reduce \
		--config-schema fake_cbc.yaml \
		--clustering-column network_chisq_weighted_snr \
		--clustering-window 0.1 \
		--db-to-insert-into $@ \
		--verbose \
		--dbs $<

H1L1V1-SGNL_INSPIRAL_INJ_C1-1241738590-3835.sqlite.gz : H1L1V1-SGNL_INSPIRAL_INJ-1241738590-3835.sqlite.gz
	stillsuit-merge-reduce \
		--config-schema fake_cbc.yaml \
		--clustering-column network_chisq_weighted_snr \
		--clustering-window 0.1 \
		--db-to-insert-into $@ \
		--verbose \
		--dbs $<

H1L1V1-SGNL_INSPIRAL_C1_LR-1241738590-3835.sqlite.gz: H1L1V1-SGNL_INSPIRAL_C1-1241738590-3835.sqlite.gz
	./assign-lr \
		--input-db $< \
		--config-schema fake_cbc.yaml \
		--output-db $@

H1L1V1-SGNL_INSPIRAL_INJ_C1_LR-1241738590-3835.sqlite.gz: H1L1V1-SGNL_INSPIRAL_INJ_C1-1241738590-3835.sqlite.gz
	./assign-lr \
		--input-db $< \
		--config-schema fake_cbc.yaml \
		--output-db $@

H1L1V1-SGNL_INSPIRAL_C2-1241738590-3835.sqlite.gz : H1L1V1-SGNL_INSPIRAL_C1_LR-1241738590-3835.sqlite.gz
	stillsuit-merge-reduce \
		--config-schema fake_cbc.yaml \
		--clustering-column likelihood \
		--clustering-window 4.0 \
		--db-to-insert-into $@ \
		--verbose \
		--dbs $<

H1L1V1-SGNL_INSPIRAL_INJ_C2-1241738590-3835.sqlite.gz : H1L1V1-SGNL_INSPIRAL_INJ_C1_LR-1241738590-3835.sqlite.gz
	stillsuit-merge-reduce \
		--config-schema fake_cbc.yaml \
		--clustering-column likelihood \
		--clustering-window 4.0 \
		--db-to-insert-into $@ \
		--verbose \
		--dbs $<

H1L1V1-SGNL_INSPIRAL_C2_FAR-1241738590-3835.sqlite.gz: H1L1V1-SGNL_INSPIRAL_C2-1241738590-3835.sqlite.gz
	./assign-far \
		--input-db $< \
		--config-schema fake_cbc.yaml \
		--output-db $@

H1L1V1-SGNL_INSPIRAL_INJ_C2_FAR-1241738590-3835.sqlite.gz: H1L1V1-SGNL_INSPIRAL_INJ_C2-1241738590-3835.sqlite.gz
	./assign-far \
		--input-db $< \
		--config-schema fake_cbc.yaml \
		--output-db $@

missed_found.png: H1L1V1-SGNL_INSPIRAL_INJ_C2_FAR-1241738590-3835.sqlite.gz
	./plot-sim \
		--input-db $< \
		--output-name $@ \
		--config-schema fake_cbc.yaml

clean:
	rm -f \
		H1L1V1-SGNL_INSPIRAL-1241738590-3835.sqlite.gz \
		H1L1V1-SGNL_INSPIRAL_C1-1241738590-3835.sqlite.gz \
		H1L1V1-SGNL_INSPIRAL_C2-1241738590-3835.sqlite.gz \
		H1L1V1-SGNL_INSPIRAL_STATS-1241738590-3835.xml.gz \
		H1L1V1-SGNL_INSPIRAL_C1_LR-1241738590-3835.sqlite.gz \
		H1L1V1-SGNL_INSPIRAL_C2_FAR-1241738590-3835.sqlite.gz \
		H1L1V1-SGNL_INSPIRAL_INJ-1241738590-3835.sqlite.gz \
		H1L1V1-SGNL_INSPIRAL_INJ_C1-1241738590-3835.sqlite.gz \
		H1L1V1-SGNL_INSPIRAL_INJ_C2-1241738590-3835.sqlite.gz \
		H1L1V1-SGNL_INSPIRAL_INJ_STATS-1241738590-3835.xml.gz \
		H1L1V1-SGNL_INSPIRAL_INJ_C1_LR-1241738590-3835.sqlite.gz \
		H1L1V1-SGNL_INSPIRAL_INJ_C2_FAR-1241738590-3835.sqlite.gz
