all : H1L1V1-SGNL_INSPIRAL_C2_FAR-1241738590-3835.sqlite.gz sgnl-sim-page.html sgnl-results-page.html

# Data are in /home/yun-jing.huang/phd/sgn/run/offline on CIT
START_TIME=1241738590
END_TIME=1241742425

H1L1V1-SGNL_INSPIRAL-1241738590-3835.sqlite.gz H1L1V1-SGNL_INSPIRAL_LIKELIHOOD_RATIO-1241738590-3835.xml.gz :
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
		--output-likelihood-file H1L1V1-SGNL_INSPIRAL_LIKELIHOOD_RATIO-1241738590-3835.xml.gz

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

H1L1V1-SGNL_INSPIRAL_MARG_LIKELIHOOD_RATIO-1241725020-760106.xml.gz : H1L1V1-SGNL_INSPIRAL_LIKELIHOOD_RATIO-1241738590-3835.xml.gz
	strike-marginalize-likelihood \
		--marginalize likelihood-ratio \
		--output $@ \
		--input $<

H1L1V1-SGNL_INSPIRAL_PRIOR_LIKELIHOOD_RATIO-1241725020-760106.xml.gz :
	sgnl-create-prior-diststats \
		--instrument H1 \
		--instrument L1 \
		--instrument V1 \
		--min-instruments 1 \
		--svd-file H1-0000_GSTLAL_SVD_BANK-1241725020-760106.xml.gz \
		--svd-file L1-0000_GSTLAL_SVD_BANK-1241725020-760106.xml.gz \
		--svd-file V1-0000_GSTLAL_SVD_BANK-1241725020-760106.xml.gz \
		--mass-model-file H1L1V1-GSTLAL_BBH_SALPETER_MASS_MODEL-0-0.h5 \
		--output-likelihood-file $@

H1L1V1-SGNL_INSPIRAL_MARG_LIKELIHOOD_RATIO_PRIOR-1241725020-760106.xml.gz : H1L1V1-SGNL_INSPIRAL_MARG_LIKELIHOOD_RATIO-1241725020-760106.xml.gz H1L1V1-SGNL_INSPIRAL_PRIOR_LIKELIHOOD_RATIO-1241725020-760106.xml.gz
	strike-marginalize-likelihood \
		--marginalize likelihood-ratio \
		--output $@ \
		--input $^

H1L1V1-SGNL_INSPIRAL_C1_LR-1241738590-3835.sqlite.gz: H1L1V1-SGNL_INSPIRAL_C1-1241738590-3835.sqlite.gz H1L1V1-SGNL_INSPIRAL_MARG_LIKELIHOOD_RATIO_PRIOR-1241725020-760106.xml.gz
	sgnl-assign-likelihood \
		--input-database-file $< \
		--config-schema fake_cbc.yaml \
		--output-database-file $@ \
		--force \
		--tmp-space _CONDOR_SCRATCH_DIR \
		--input-likelihood-file H1L1V1-SGNL_INSPIRAL_MARG_LIKELIHOOD_RATIO_PRIOR-1241725020-760106.xml.gz

H1L1V1-SGNL_INSPIRAL_INJ_C1_LR-1241738590-3835.sqlite.gz: H1L1V1-SGNL_INSPIRAL_INJ_C1-1241738590-3835.sqlite.gz H1L1V1-SGNL_INSPIRAL_MARG_LIKELIHOOD_RATIO_PRIOR-1241725020-760106.xml.gz
	sgnl-assign-likelihood \
		--input-database-file  $< \
		--config-schema fake_cbc.yaml \
		--output-database-file $@ \
		--force \
		--tmp-space _CONDOR_SCRATCH_DIR \
		--input-likelihood-file H1L1V1-SGNL_INSPIRAL_MARG_LIKELIHOOD_RATIO_PRIOR-1241725020-760106.xml.gz

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

H1L1V1-SGNL_INSPIRAL_RANK_STAT_PDF-1241725020-760106.xml.gz : H1L1V1-SGNL_INSPIRAL_MARG_LIKELIHOOD_RATIO_PRIOR-1241725020-760106.xml.gz
	strike-calc-rank-pdfs \
		--num-samples 1048576 \
		--num-cores 1 \
		--output-rankingstatpdf-file $@ \
		-i $<

H1L1V1-SGNL_INSPIRAL_RANK_STAT_PDF_E1-1241725020-760106.xml.gz : H1L1V1-SGNL_INSPIRAL_C1_LR-1241738590-3835.sqlite.gz H1L1V1-SGNL_INSPIRAL_RANK_STAT_PDF-1241725020-760106.xml.gz
	sgnl-extinct-bin \
		-s fake_cbc.yaml \
		--reset-zerolag \
		--input-database-file $< \
		--output-rankingstatpdf-file $@ \
		--input-rankingstatpdf-file H1L1V1-SGNL_INSPIRAL_RANK_STAT_PDF-1241725020-760106.xml.gz

H1L1V1-SGNL_INSPIRAL_MARG_RANK_STAT_PDF-1241725020-760106.xml.gz : H1L1V1-SGNL_INSPIRAL_RANK_STAT_PDF_E1-1241725020-760106.xml.gz
	strike-marginalize-likelihood \
		--marginalize ranking-stat-pdf \
		--output $@ \
		--input $<

H1L1V1-SGNL_INSPIRAL_POST_RANK_STAT_PDF-1241725020-760106.xml.gz : H1L1V1-SGNL_INSPIRAL_C2-1241738590-3835.sqlite.gz H1L1V1-SGNL_INSPIRAL_MARG_RANK_STAT_PDF-1241725020-760106.xml.gz
	sgnl-extinct-bin \
		-s fake_cbc.yaml \
		--input-database-file $< \
		--output-rankingstatpdf-file $@ \
		--input-rankingstatpdf-file H1L1V1-SGNL_INSPIRAL_MARG_RANK_STAT_PDF-1241725020-760106.xml.gz

H1L1V1-SGNL_INSPIRAL_C2_FAR-1241738590-3835.sqlite.gz: H1L1V1-SGNL_INSPIRAL_C2-1241738590-3835.sqlite.gz H1L1V1-SGNL_INSPIRAL_POST_RANK_STAT_PDF-1241725020-760106.xml.gz
	sgnl-assign-far \
		--input-database-file $< \
		--config-schema fake_cbc.yaml \
		--output-database-file $@ \
		--force \
		--tmp-space _CONDOR_SCRATCH_DIR \
		--input-likelihood-file H1L1V1-SGNL_INSPIRAL_POST_RANK_STAT_PDF-1241725020-760106.xml.gz

H1L1V1-SGNL_INSPIRAL_INJ_C2_FAR-1241738590-3835.sqlite.gz: H1L1V1-SGNL_INSPIRAL_INJ_C2-1241738590-3835.sqlite.gz H1L1V1-SGNL_INSPIRAL_POST_RANK_STAT_PDF-1241725020-760106.xml.gz
	sgnl-assign-far \
		--input-database-file $< \
		--config-schema fake_cbc.yaml \
		--output-database-file $@ \
		--force \
		--tmp-space _CONDOR_SCRATCH_DIR \
		--input-likelihood-file H1L1V1-SGNL_INSPIRAL_POST_RANK_STAT_PDF-1241725020-760106.xml.gz

sgnl-sim-page.html: H1L1V1-SGNL_INSPIRAL_INJ_C2_FAR-1241738590-3835.sqlite.gz
	sgnl-sim-page \
		--input-db $< \
		--config-schema fake_cbc.yaml \
		--output-html $@

sgnl-results-page.html: H1L1V1-SGNL_INSPIRAL_C2_FAR-1241738590-3835.sqlite.gz
	sgnl-results-page --input-db $< --output-html $@ --config-schema fake_cbc.yaml

clean:
	rm -f \
		H1L1V1-SGNL_INSPIRAL-1241738590-3835.sqlite.gz \
		H1L1V1-SGNL_INSPIRAL_C1-1241738590-3835.sqlite.gz \
		H1L1V1-SGNL_INSPIRAL_C2-1241738590-3835.sqlite.gz \
		H1L1V1-SGNL_INSPIRAL_LIKELIHOOD_RATIO-1241738590-3835.xml.gz \
		H1L1V1-SGNL_INSPIRAL_C1_LR-1241738590-3835.sqlite.gz \
		H1L1V1-SGNL_INSPIRAL_C2_FAR-1241738590-3835.sqlite.gz \
		H1L1V1-SGNL_INSPIRAL_INJ-1241738590-3835.sqlite.gz \
		H1L1V1-SGNL_INSPIRAL_INJ_C1-1241738590-3835.sqlite.gz \
		H1L1V1-SGNL_INSPIRAL_INJ_C2-1241738590-3835.sqlite.gz \
		H1L1V1-SGNL_INSPIRAL_INJ_LIKELIHOOD_RATIO-1241738590-3835.xml.gz \
		H1L1V1-SGNL_INSPIRAL_INJ_C1_LR-1241738590-3835.sqlite.gz \
		H1L1V1-SGNL_INSPIRAL_INJ_C2_FAR-1241738590-3835.sqlite.gz \
		H1L1V1-SGNL_INSPIRAL_RANK_STAT_PDF-1241725020-760106.xml.gz \
		H1L1V1-SGNL_INSPIRAL_MARG_RANK_STAT_PDF-1241725020-760106.xml.gz \
		H1L1V1-SGNL_INSPIRAL_MARG_LIKELIHOOD_RATIO_PRIOR-1241725020-760106.xml.gz \
		H1L1V1-SGNL_INSPIRAL_PRIOR_LIKELIHOOD_RATIO-1241725020-760106.xml.gz \
		H1L1V1-SGNL_INSPIRAL_RANK_STAT_PDF_E1-1241725020-760106.xml.gz \
		H1L1V1-SGNL_INSPIRAL_MARG_LIKELIHOOD_RATIO-1241725020-760106.xml.gz \
		H1L1V1-SGNL_INSPIRAL_POST_RANK_STAT_PDF-1241725020-760106.xml.gz
		sgnl-sim-page.html \
