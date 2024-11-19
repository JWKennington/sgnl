all : H1L1V1-SGNL_INSPIRAL_C2-1241738590-3835.sqlite

H1L1V1-SGNL_INSPIRAL-1241738590-3835.sqlite :
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


H1L1V1-SGNL_INSPIRAL_C1-1241738590-3835.sqlite : H1L1V1-SGNL_INSPIRAL-1241738590-3835.sqlite
	stillsuit-merge-reduce \
		--config-schema fake_cbc.yaml \
		--clustering-column network_chisq_weighted_snr \
		--clustering-window 0.1 \
		--db-to-insert-into $@ \
		--verbose \
		--dbs $<

H1L1V1-SGNL_INSPIRAL_C2-1241738590-3835.sqlite : H1L1V1-SGNL_INSPIRAL_C1-1241738590-3835.sqlite
	stillsuit-merge-reduce \
		--config-schema fake_cbc.yaml \
		--clustering-column likelihood \
		--clustering-window 4.0 \
		--db-to-insert-into $@ \
		--verbose \
		--dbs $<

clean:
	rm -f H1L1V1-SGNL_INSPIRAL-1241738590-3835.sqlite H1L1V1-SGNL_INSPIRAL_C1-1241738590-3835.sqlite H1L1V1-SGNL_INSPIRAL_C2-1241738590-3835.sqlite H1L1V1-SGNL_INSPIRAL_STATS-1241738590-3835.xml.gz
