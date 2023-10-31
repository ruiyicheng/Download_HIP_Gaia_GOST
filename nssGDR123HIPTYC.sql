--The following code is used for downloading the Gaia DR1 DR2 DR3 astrometry for non-single stars combined with their HIP and TYC ids.
--The code should be used in the ADQL interface at https://gea.esac.esa.int/archive/ -> SEARCH -> Advanced(ADQL)
SELECT 
dr3.source_id AS dr3_source_id,
dr3.phot_g_mean_mag AS Gmag_dr3,
dr3.bp_rp AS bp_rp_dr3,
dr3.teff_gspphot AS teff_dr3,
dr3.ra AS dr3_ra,
dr3.ra_error AS dr3_ra_error,
dr3.dec AS dr3_dec,
dr3.dec_error AS dr3_dec_error,
dr3.parallax AS dr3_parallax,
dr3.parallax_error AS dr3_parallax_error,
dr3.pmra AS dr3_pmra,
dr3.pmra_error AS dr3_pmra_error,
dr3.pmdec AS dr3_pmdec,
dr3.pmdec_error AS dr3_pmdec_error,
dr3.ruwe AS dr3_ruwe,
dr3.radial_velocity AS dr3_radial_velocity,
dr3.radial_velocity_error AS dr3_radial_velocity_error,
dr3.ra_dec_corr AS dr3_ra_dec_corr,
dr3.ra_parallax_corr AS dr3_ra_parallax_corr,
dr3.ra_pmra_corr AS dr3_ra_pmra_corr,
dr3.ra_pmdec_corr AS dr3_ra_pmdec_corr,
dr3.dec_parallax_corr AS dr3_dec_parallax_corr,
dr3.dec_pmra_corr AS dr3_dec_pmra_corr,
dr3.dec_pmdec_corr AS dr3_dec_pmdec_corr,
dr3.parallax_pmra_corr AS dr3_parallax_pmra_corr,
dr3.parallax_pmdec_corr AS dr3_parallax_pmdec_corr,
dr3.pmra_pmdec_corr AS dr3_pmra_pmdec_corr,
dr2.source_id AS dr2_source_id,
dr2.ra AS dr2_ra,
dr2.ra_error AS dr2_ra_error,
dr2.dec AS dr2_dec,
dr2.dec_error AS dr2_dec_error,
dr2.parallax AS dr2_parallax,
dr2.parallax_error AS dr2_parallax_error,
dr2.pmra AS dr2_pmra,
dr2.pmra_error AS dr2_pmra_error,
dr2.pmdec AS dr2_pmdec,
dr2.pmdec_error AS dr2_pmdec_error,
dr2.radial_velocity AS dr2_radial_velocity,
dr2.radial_velocity_error AS dr2_radial_velocity_error,
dr2.ra_dec_corr AS dr2_ra_dec_corr,
dr2.ra_parallax_corr AS dr2_ra_parallax_corr,
dr2.ra_pmra_corr AS dr2_ra_pmra_corr,
dr2.ra_pmdec_corr AS dr2_ra_pmdec_corr,
dr2.dec_parallax_corr AS dr2_dec_parallax_corr,
dr2.dec_pmra_corr AS dr2_dec_pmra_corr,
dr2.dec_pmdec_corr AS dr2_dec_pmdec_corr,
dr2.parallax_pmra_corr AS dr2_parallax_pmra_corr,
dr2.parallax_pmdec_corr AS dr2_parallax_pmdec_corr,
dr2.pmra_pmdec_corr AS dr2_pmra_pmdec_corr,
dr1.source_id AS dr1_source_id,
dr1.ra AS dr1_ra,
dr1.ra_error AS dr1_ra_error,
dr1.dec AS dr1_dec,
dr1.dec_error AS dr1_dec_error,
dr1.parallax AS dr1_parallax,
dr1.parallax_error AS dr1_parallax_error,
dr1.pmra AS dr1_pmra,
dr1.pmra_error AS dr1_pmra_error,
dr1.pmdec AS dr1_pmdec,
dr1.pmdec_error AS dr1_pmdec_error,
dr1.ra_dec_corr AS dr1_ra_dec_corr,
dr1.ra_parallax_corr AS dr1_ra_parallax_corr,
dr1.ra_pmra_corr AS dr1_ra_pmra_corr,
dr1.ra_pmdec_corr AS dr1_ra_pmdec_corr,
dr1.dec_parallax_corr AS dr1_dec_parallax_corr,
dr1.dec_pmra_corr AS dr1_dec_pmra_corr,
dr1.dec_pmdec_corr AS dr1_dec_pmdec_corr,
dr1.parallax_pmra_corr AS dr1_parallax_pmra_corr,
dr1.parallax_pmdec_corr AS dr1_parallax_pmdec_corr,
dr1.pmra_pmdec_corr AS dr1_pmra_pmdec_corr,
dr2_hip.original_ext_source_id AS hip_id,
dr2_tyc.original_ext_source_id AS tyc_id

FROM gaiadr3.nss_two_body_orbit AS tbd
JOIN gaiadr3.gaia_source AS dr3 
ON dr3.source_id = tbd.source_id
JOIN gaiadr3.dr2_neighbourhood AS dr3_dr2
ON dr3.source_id = dr3_dr2.dr3_source_id
JOIN gaiadr2.gaia_source AS dr2
ON dr3_dr2.dr2_source_id = dr2.source_id
JOIN gaiadr2.dr1_neighbourhood AS dr2_dr1
ON dr2_dr1.dr2_source_id = dr2.source_id
JOIN gaiadr1.gaia_source AS dr1
ON dr2_dr1.dr1_source_id = dr1.source_id
LEFT JOIN gaiadr2.hipparcos2_neighbourhood AS dr2_hip
ON dr2_hip.source_id = dr2.source_id
LEFT JOIN gaiadr2.tycho2_neighbourhood AS dr2_tyc
ON dr2_tyc.source_id = dr2.source_id
ORDER BY dr3.source_id ASC