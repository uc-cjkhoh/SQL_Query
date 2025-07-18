-- Objection: For each country's failed/rejected call, find out the msisdn's prefix distribution in percentage or count 
WITH
    -- Find each country's dial number
    country_codes AS (
        SELECT
            mcc_ref,
            country_code,
            LENGTH (CAST(country_code AS STRING)) as dial_no_len
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
            LENGTH (CAST(cc.country_code AS STRING)) as dial_no_len,                                                    -- calculate the dial number length (eg. +65 = 2)
            cdr.clg_num,
            LENGTH (CAST(cdr.clg_num AS STRING)) AS clg_length,                                                         -- calculate the calling number's length
            CAST(CAST(cld_num AS BIGINT) AS STRING) AS cld_num,                                                         -- if first letter if called number is '0', remove it 
            LENGTH (CAST(CAST(cld_num AS BIGINT) AS STRING)) AS cld_length,
            LENGTH (CAST(cdr.clg_num AS STRING)) - LENGTH (CAST(CAST(cld_num AS BIGINT) AS STRING)) AS len_diff         -- calculate the called number's length
        FROM
            roam352_report.data_cdr cdr
            JOIN country_codes cc ON (cdr.clg_mcc_ref = cc.mcc_ref)
        WHERE
            cdr.service_type = 301                                                                                      -- ICC service
            AND cdr.status = '10001f'                                                                                   -- rejected tx (ACM ?)
            AND cdr.par_bound_type = 1                                                                                  -- inbound only
            AND cdr.par_month = 202506                                                                                  -- specific date 
            AND (
                (                                                                                                       -- the length difference between clg_num and cld_num must equal to 0 or length of corresponding dial number 
                    LENGTH (CAST(cdr.clg_num AS STRING)) - LENGTH (CAST(CAST(cld_num AS BIGINT) AS STRING)) = 0
                )
                OR                                                                                                       
                (
                    LENGTH (CAST(cdr.clg_num AS STRING)) - LENGTH (CAST(CAST(cld_num AS BIGINT) AS STRING)) = LENGTH (CAST(cc.country_code AS STRING))
                )                                                                                                        
            )
            AND LEFT (
                CAST(CAST(cld_num AS BIGINT) AS STRING),                                                                -- check if leading number of called number similar to dial number
                LENGTH (CAST(cc.country_code AS STRING)) -                                                              -- eg.  if length difference of clg_num and cld_num, "len_diff" is 2,
                (
                    LENGTH (CAST(cdr.clg_num AS STRING)) - LENGTH (CAST(CAST(cld_num AS BIGINT) AS STRING))
                )                                                                                                       --      select first first(cld_num, dial number length - len_diff),
            ) = RIGHT (                                                                                                 --      compare if it is the same as last(dial_no, dial number length - len_diff)
                CAST(cc.country_code AS STRING),
                LENGTH (CAST(cc.country_code AS STRING)) - (
                    LENGTH (CAST(cdr.clg_num AS STRING)) - LENGTH (CAST(CAST(cld_num AS BIGINT) AS STRING))
                )
            )
            AND LENGTH (CAST(cdr.cld_num AS STRING)) <= 15                                                              -- only if called number follow msisdn standard (limited to 15 digits)
            AND LEFT (
                CAST(cdr.clg_num AS STRING),
                LENGTH (CAST(cc.country_code AS STRING))
            ) = CAST(cc.country_code AS STRING)                                                                         -- check if calling number start with their own dial number (eg. Singapore caller must start with +65) 
            AND LENGTH (CAST(cc.country_code AS STRING)) >= (                                                           -- check if dial number's length greater or equal than the length difference of clg_num and cld_num
                LENGTH (CAST(cdr.clg_num AS STRING)) - LENGTH (CAST(CAST(cld_num AS BIGINT) AS STRING))
            )
        GROUP BY
            cdr.clg_mcc_ref,
            cc.country_code,
            LENGTH (CAST(cc.country_code AS STRING)),
            cdr.clg_num,
            CAST(CAST(cld_num AS BIGINT) AS STRING)
    ),
    -- Calculate how many transactions exists by country and msisdn length
    traffic AS (
        SELECT
            clg_mcc_ref,
            LENGTH (CAST(clg_num AS STRING)) AS clg_num_len,
            COUNT(*) AS traffic_count
        FROM
            base_tbl
        GROUP BY
            clg_mcc_ref,
            LENGTH (CAST(clg_num AS STRING))
    ),
    -- List all possible operators' mobile prefix in each country
    operator_mobile_prefix AS (
        SELECT
            clg_mcc_ref,
            dial_no, 
            MIN(SUBSTR (regis_no, 1, 1)) AS min_p1,
            MAX(SUBSTR (regis_no, 1, 1)) AS max_p1,
            MIN(SUBSTR (regis_no, 2, 1)) AS min_p2,
            MAX(SUBSTR (regis_no, 2, 1)) AS max_p2,
            MIN(SUBSTR (regis_no, 3, 1)) AS min_p3,
            MAX(SUBSTR (regis_no, 3, 1)) AS max_p3
        FROM
            (
                SELECT
                    cdr.clg_mcc_ref,
                    cc.country_code AS dial_no,
                    SUBSTR (
                        CAST(cdr.clg_num AS STRING),
                        LENGTH (CAST(cc.country_code AS STRING)) + 1,
                        LENGTH (CAST(cdr.clg_num AS STRING)) - LENGTH (CAST(cc.country_code AS STRING))
                    ) AS regis_no
                FROM
                    roam352_report.data_cdr cdr
                    JOIN country_codes cc ON (cdr.clg_mcc_ref = cc.mcc_ref)
                WHERE
                    cdr.par_month = 202506
                    AND cdr.service_type = 301
                    AND cdr.par_bound_type = 1
                GROUP BY
                    cdr.clg_mcc_ref,
                    cc.country_code,
                    regis_no
            ) tbl
        GROUP BY
            clg_mcc_ref,
            dial_no 
    )

 

SELECT
    tbl.clg_mcc_ref,
    tbl.clg_length,
    tbl.dial_no,
    tbl.dial_no_len,
    tbl.len_diff,
    tbl.total_count,
    100 * (tbl.total_count / t.traffic_count) as percentage
FROM
    (
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
    JOIN traffic t ON (
        t.clg_mcc_ref = tbl.clg_mcc_ref
        AND t.clg_num_len = tbl.clg_length
    )
ORDER BY
    tbl.clg_mcc_ref,
    tbl.clg_length,
    tbl.total_count DESC