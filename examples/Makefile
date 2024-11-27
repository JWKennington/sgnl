all : H1L1V1-SGNL_INSPIRAL_C2_FAR-1241738590-3835.sqlite.gz missed_found.png

# Data are in /home/yun-jing.huang/phd/sgn/run/offline on CIT
START_TIME=1241738590
END_TIME=1241742425

H1L1V1-SGNL_INSPIRAL-1241738590-3835.sqlite.gz H1L1V1-SGNL_INSPIRAL_STATS-1241738590-3835.xml.gz :
	sgnl-inspiral \
		--ht-gate-threshold 50.0 \
		--gps-start-time ${START_TIME} \
		--gps-end-time ${END_TIME} \
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
		--trigger-output H1L1V1-SGNL_INSPIRAL-1241738590-3835.sqlite.gz \
		--torch-dtype float32 \
		--trigger-finding-duration 1 \
		--event-config fake_cbc.yaml \
		--ranking-stat-output H1L1V1-SGNL_INSPIRAL_STATS-1241738590-3835.xml.gz

# Inj data are in /home/yun-jing.huang/phd/sgn/run/offline-injections on CIT
H1L1V1-SGNL_INSPIRAL_INJ-1241738590-3835.sqlite.gz :
	sgnl-inspiral \
		--ht-gate-threshold 50.0 \
		--gps-start-time ${START_TIME} \
		--gps-end-time ${END_TIME} \
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

H1L1V1-SGNL_INSPIRAL_MARG_STATS-1241725020-760106.xml.gz : H1L1V1-SGNL_INSPIRAL_STATS-1241738590-3835.xml.gz
	strike-marginalize-likelihood \
		--marginalize ranking-stat \
		--output $@ \
		--input $<

H1L1V1-SGNL_INSPIRAL_PRIOR_DIST_STATS-1241725020-760106.xml.gz : 
	sgnl-create-prior-diststats \
		--instrument H1 \
		--instrument L1 \
		--instrument V1 \
		--min-instruments 1 \
		--svd-file H1-0000_GSTLAL_SVD_BANK-1241725020-760106.xml.gz \
		--svd-file L1-0000_GSTLAL_SVD_BANK-1241725020-760106.xml.gz \
		--svd-file V1-0000_GSTLAL_SVD_BANK-1241725020-760106.xml.gz \
		--mass-model-file H1L1V1-GSTLAL_BBH_SALPETER_MASS_MODEL-0-0.h5 \
		--write-likelihood $@

H1L1V1-SGNL_INSPIRAL_MARG_STATS_PRIOR-1241725020-760106.xml.gz : H1L1V1-SGNL_INSPIRAL_MARG_STATS-1241725020-760106.xml.gz H1L1V1-SGNL_INSPIRAL_PRIOR_DIST_STATS-1241725020-760106.xml.gz
	strike-marginalize-likelihood \
		--marginalize ranking-stat \
		--output $@ \
		--input $^

H1L1V1-SGNL_INSPIRAL_C1_LR-1241738590-3835.sqlite.gz: H1L1V1-SGNL_INSPIRAL_C1-1241738590-3835.sqlite.gz H1L1V1-SGNL_INSPIRAL_MARG_STATS_PRIOR-1241725020-760106.xml.gz
	sgnl-assign-likelihood \
		--input-database $< \
		--config-schema fake_cbc.yaml \
		--output-database $@ \
		--force \
		--tmp-space _CONDOR_SCRATCH_DIR \
		--likelihood-url H1L1V1-SGNL_INSPIRAL_MARG_STATS_PRIOR-1241725020-760106.xml.gz

H1L1V1-SGNL_INSPIRAL_INJ_C1_LR-1241738590-3835.sqlite.gz: H1L1V1-SGNL_INSPIRAL_INJ_C1-1241738590-3835.sqlite.gz H1L1V1-SGNL_INSPIRAL_MARG_STATS_PRIOR-1241725020-760106.xml.gz
	sgnl-assign-likelihood \
		--input-database  $< \
		--config-schema fake_cbc.yaml \
		--output-database $@ \
		--force \
		--tmp-space _CONDOR_SCRATCH_DIR \
		--likelihood-url H1L1V1-SGNL_INSPIRAL_MARG_STATS_PRIOR-1241725020-760106.xml.gz

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

H1L1V1-SGNL_INSPIRAL_DIST_STAT_PDFS-1241725020-760106.xml.gz : H1L1V1-SGNL_INSPIRAL_MARG_STATS_PRIOR-1241725020-760106.xml.gz
	strike-calc-rank-pdfs \
		--ranking-stat-samples 1048576 \
		--num-cores 8 \
		--output $@ \
		-i $<

H1L1V1-SGNL_INSPIRAL_DIST_STAT_PDFS_E1-1241725020-760106.xml.gz : H1L1V1-SGNL_INSPIRAL_C1_LR-1241738590-3835.sqlite.gz H1L1V1-SGNL_INSPIRAL_DIST_STAT_PDFS-1241725020-760106.xml.gz
	sgnl-extinct-bin \
		-s fake_cbc.yaml \
		--reset-zerolag \
	   	--input-trigger-file $< \
	   	--output-diststatpdf-file $@ \
		--input-diststatpdf-file H1L1V1-SGNL_INSPIRAL_DIST_STAT_PDFS-1241725020-760106.xml.gz

H1L1V1-SGNL_INSPIRAL_MARG_DIST_STAT_PDFS-1241725020-760106.xml.gz : H1L1V1-SGNL_INSPIRAL_DIST_STAT_PDFS_E1-1241725020-760106.xml.gz
	strike-marginalize-likelihood \
		--marginalize ranking-stat-pdf \
		--output $@ \
		--input $<

H1L1V1-SGNL_INSPIRAL_POST_DIST_STAT_PDFS-1241725020-760106.xml.gz : H1L1V1-SGNL_INSPIRAL_C2-1241738590-3835.sqlite.gz H1L1V1-SGNL_INSPIRAL_MARG_DIST_STAT_PDFS-1241725020-760106.xml.gz
	sgnl-extinct-bin \
		-s fake_cbc.yaml \
	   	--input-trigger-file $< \
	   	--output-diststatpdf-file $@ \
		--input-diststatpdf-file H1L1V1-SGNL_INSPIRAL_MARG_DIST_STAT_PDFS-1241725020-760106.xml.gz

H1L1V1-SGNL_INSPIRAL_C2_FAR-1241738590-3835.sqlite.gz: H1L1V1-SGNL_INSPIRAL_C2-1241738590-3835.sqlite.gz H1L1V1-SGNL_INSPIRAL_POST_DIST_STAT_PDFS-1241725020-760106.xml.gz
	sgnl-assign-far \
		--input-database $< \
		--config-schema fake_cbc.yaml \
		--output-database $@ \
		--force \
		--tmp-space _CONDOR_SCRATCH_DIR \
		--background-bins-file H1L1V1-SGNL_INSPIRAL_POST_DIST_STAT_PDFS-1241725020-760106.xml.gz

H1L1V1-SGNL_INSPIRAL_INJ_C2_FAR-1241738590-3835.sqlite.gz: H1L1V1-SGNL_INSPIRAL_INJ_C2-1241738590-3835.sqlite.gz H1L1V1-SGNL_INSPIRAL_POST_DIST_STAT_PDFS-1241725020-760106.xml.gz
	sgnl-assign-far \
		--input-database $< \
		--config-schema fake_cbc.yaml \
		--output-database $@ \
		--force \
		--tmp-space _CONDOR_SCRATCH_DIR \
		--background-bins-file H1L1V1-SGNL_INSPIRAL_POST_DIST_STAT_PDFS-1241725020-760106.xml.gz

missed_found.png: H1L1V1-SGNL_INSPIRAL_INJ_C2_FAR-1241738590-3835.sqlite.gz
	./plot-sim \
		--input-db $< \
		--output-name $@ \
		--config-schema fake_cbc.yaml \

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
		H1L1V1-SGNL_INSPIRAL_INJ_C2_FAR-1241738590-3835.sqlite.gz \
		H1L1V1-SGNL_INSPIRAL_DIST_STAT_PDFS-1241725020-760106.xml.gz \
		H1L1V1-SGNL_INSPIRAL_MARG_DIST_STAT_PDFS-1241725020-760106.xml.gz \
		H1L1V1-SGNL_INSPIRAL_MARG_STATS_PRIOR-1241725020-760106.xml.gz \
		H1L1V1-SGNL_INSPIRAL_PRIOR_DIST_STATS-1241725020-760106.xml.gz \
		H1L1V1-SGNL_INSPIRAL_DIST_STAT_PDFS_E1-1241725020-760106.xml.gz \
		H1L1V1-SGNL_INSPIRAL_MARG_STATS-1241725020-760106.xml.gz \
		H1L1V1-SGNL_INSPIRAL_POST_DIST_STAT_PDFS-1241725020-760106.xml.gz \
		missed_found.png
