-- Objection: For each country's failed/rejected call, find out the msisdn's prefix distribution in percentage or count 

WITH  
    -- Find each country's dial number
    country_codes AS (
        SELECT
            mcc_ref,
            country_code,
            LENGTH(CAST(country_code AS STRING)) as dial_no_len 
        FROM
            roam352_report.sys_country_profile
        GROUP BY
            mcc_ref,
            country_code 
    ),
    -- The base dataset to focus on, filter by conditions (where statements)
    base_tbl AS (
        SELECT 
            cdr.clg_mcc_ref,
            cc.country_code AS dial_no,
            LENGTH(CAST(cc.country_code AS STRING)) as dial_no_len,
            cdr.clg_num,
            LENGTH(CAST(cdr.clg_num AS STRING)) AS clg_length,
            cdr.cld_num,
            LENGTH(CAST(cdr.cld_num AS STRING)) AS cld_length
        FROM 
            roam352_report.data_cdr cdr
            JOIN country_codes cc ON (cdr.clg_mcc_ref = cc.mcc_ref)
        WHERE
            cdr.service_type = 301                                                                                              -- ICC service
            AND cdr.status = '10001f'                                                                                           -- rejected tx (ACM ?)
            AND cdr.par_bound_type = 1                                                                                          -- inbound only
            AND cdr.par_month = 202506                                                                                          -- specific date 
            AND (
                (LENGTH(CAST(cdr.clg_num AS STRING)) - LENGTH(CAST(cdr.cld_num AS STRING)) >= 0) AND                            -- called number not longer than calling number
                (LENGTH(CAST(cdr.clg_num AS STRING)) - LENGTH(CAST(cdr.cld_num AS STRING)) <= 3)                                -- and their difference not longer than dial number
            )
            AND LEFT(
                    CAST(cdr.cld_num AS STRING),                                                                                -- check if leading number of called number similar to dial number
                    LENGTH(CAST(cc.country_code AS STRING)) -                                                                   -- eg.  if length difference of clg_num and cld_num, "len_diff" is 2,
                    (LENGTH(CAST(cdr.clg_num AS STRING)) - LENGTH(CAST(cdr.cld_num AS STRING)))                                 --      select first first(cld_num, dial number length - len_diff),
                ) = RIGHT(                                                                                                      --      compare if it is the same as last(dial_no, dial number length - len_diff)
                    CAST(cc.country_code AS STRING),
                    LENGTH(CAST(cc.country_code AS STRING)) -                                                                   
                    (LENGTH(CAST(cdr.clg_num AS STRING)) - LENGTH(CAST(cdr.cld_num AS STRING)))   
                )
            AND LENGTH(CAST(cdr.cld_num AS STRING)) <= 15                                                                       -- only if called number follow msisdn standard (limited to 15 digits)
            AND LEFT(CAST(cdr.clg_num AS STRING), LENGTH(CAST(cc.country_code AS STRING))) = CAST(cc.country_code AS STRING)    -- check if calling number start with their own dial number (eg. Singapore caller must start with +65) 
        GROUP BY 
            cdr.clg_mcc_ref,
            cc.country_code,
            LENGTH(CAST(cc.country_code AS STRING)),
            cdr.clg_num,
            cdr.cld_num 
    ),
    -- Calculate how many transaction each country has
    traffic AS (
        SELECT 
            clg_mcc_ref,
            LENGTH(CAST(clg_num AS STRING)) AS clg_num_len,
            COUNT(*) AS traffic_count
        FROM 
            base_tbl
        GROUP BY 
            clg_mcc_ref,
            LENGTH(CAST(clg_num AS STRING))
    )


SELECT 
    tbl.clg_mcc_ref, 
    tbl.clg_length,
    tbl.dial_no,
    tbl.dial_no_len,
    tbl.len_diff,
    tbl.total_count,
    100 * (tbl.total_count / t.traffic_count) as percentage
FROM (
    SELECT
        clg_mcc_ref,
        clg_length,
        dial_no,
        dial_no_len,
        clg_length - cld_length AS len_diff,
        COUNT(*) as total_count
    FROM 
        base_tbl
    GROUP BY
        clg_mcc_ref,
        clg_length,
        dial_no,
        dial_no_len,
        len_diff
) tbl 
JOIN traffic t ON (t.clg_mcc_ref = tbl.clg_mcc_ref AND t.clg_num_len = tbl.clg_length)  
ORDER BY
    tbl.clg_mcc_ref,
    tbl.clg_length,
    tbl.total_count
DESC
