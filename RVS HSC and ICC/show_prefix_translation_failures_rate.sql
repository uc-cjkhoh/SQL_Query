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
    -- Base dataset to extract possible ndc
    refer_tx_for_ndc AS (
        SELECT
            cdr.clg_mcc_ref,
            cc.country_code AS dial_no,
            LENGTH(CAST(cdr.clg_num AS STRING)) AS clg_len,
            SUBSTR (
                CAST(cdr.clg_num AS STRING),
                LENGTH (CAST(cc.country_code AS STRING)) + 1,
                LENGTH (CAST(cdr.clg_num AS STRING)) - LENGTH (CAST(cc.country_code AS STRING))
            ) AS regis_no
        FROM
            roam352_report.data_cdr cdr
            JOIN country_codes cc ON (cdr.clg_mcc_ref = cc.mcc_ref)
        WHERE 
            cdr.service_type = 301
            AND cdr.par_bound_type = 1
            AND cdr.par_month = 202506  
        GROUP BY
            cdr.clg_mcc_ref,
            cc.country_code,
            LENGTH(CAST(cdr.clg_num AS STRING)),
            regis_no
    ),
    -- List all possible operators' mobile prefix in each country 
    first_x_prefix AS (
        SELECT 
            clg_mcc_ref,
            dial_no,
            clg_len,
            SUBSTR(regis_no, 1, 2) AS p1,
            SUBSTR(regis_no, 2, 2) AS p2,
            SUBSTR(regis_no, 3, 2) AS p3,
            SUBSTR(regis_no, 4, 2) AS p4
        FROM 
            refer_tx_for_ndc
    ),
    -- Suggest possible ndc length
    suggest_len AS (  
        SELECT 
            clg_mcc_ref, 
            dial_no, 
            clg_len,
            COALESCE(MIN(suggest_prefix), 3) AS suggest_len
        FROM (
            SELECT 
                clg_mcc_ref, 
                dial_no, 
                clg_len,
                CASE 
                    WHEN p1_count = 1 THEN 1
                    WHEN p2_count = 1 THEN 2
                    WHEN p3_count = 1 THEN 3 
                END AS suggest_prefix
            FROM (
                SELECT 
                    clg_mcc_ref,
                    dial_no,  
                    clg_len,
                    COUNT(*) OVER (PARTITION BY clg_mcc_ref, dial_no, p1) as p1_count,
                    COUNT(*) OVER (PARTITION BY clg_mcc_ref, dial_no, p2) as p2_count,
                    COUNT(*) OVER (PARTITION BY clg_mcc_ref, dial_no, p3) as p3_count,
                    COUNT(*) OVER (PARTITION BY clg_mcc_ref, dial_no, p4) as p4_count
                FROM 
                    first_x_prefix 
            ) tbl 
        ) tbl 
        GROUP BY 
            clg_mcc_ref,
            dial_no,
            clg_len
        HAVING 
            MIN(suggest_prefix) > 0
    ),
    -- Suggested ndc 
    suggest_ndc AS (
       SELECT 
            ndc.clg_mcc_ref, 
            ndc.dial_no,
            ndc.clg_len,
            LEFT(ndc.regis_no, sl.suggest_len) AS ndc_code
        FROM 
            refer_tx_for_ndc ndc
            JOIN suggest_len sl ON (
                ndc.clg_mcc_ref = sl.clg_mcc_ref 
                AND ndc.dial_no = sl.dial_no
            )
        GROUP BY
            ndc.clg_mcc_ref,
            ndc.dial_no,
            ndc.clg_len,
            LEFT(ndc.regis_no, sl.suggest_len)  
    ),
    -- The base dataset to focus on, filter by conditions (where statements)
    base_tbl AS (
        SELECT
            cdr.clg_mcc_ref,
            cc.country_code AS dial_no,
            cdr.clg_num,
            CAST(CAST(cld_num AS BIGINT) AS STRING) AS cld_num, 
            ndc.ndc_code,                                                                                                                                       -- if first letter if called number is '0', remove it 
            LENGTH (CAST(CAST(cld_num AS BIGINT) AS STRING)) AS cld_length
        FROM
            roam352_report.data_cdr cdr
            JOIN country_codes cc ON (cdr.clg_mcc_ref = cc.mcc_ref)
            JOIN suggest_ndc ndc ON (cdr.clg_mcc_ref = ndc.clg_mcc_ref)
        WHERE
            cdr.service_type = 301                                                                                                                              -- ICC service
            AND cdr.status = '10001f'                                                                                                                           -- rejected tx (ACM ?)
            AND cdr.par_bound_type = 1                                                                                                                          -- inbound only
            AND cdr.par_month = 202506                                                                                                                          -- time interval
            AND LENGTH (CAST(cdr.cld_num AS STRING)) <= 15                                                                                                      -- only if called number follow msisdn standard (limited to 15 digits)
            AND (                                                                                                                                               -- the length difference between clg_num and cld_num must equal to 0 or length of corresponding dial number 
                LENGTH (CAST(cdr.clg_num AS STRING)) - LENGTH (CAST(CAST(cld_num AS BIGINT) AS STRING)) = 0
                OR LENGTH (CAST(cdr.clg_num AS STRING)) - LENGTH (CAST(CAST(cld_num AS BIGINT) AS STRING)) = LENGTH (CAST(cc.country_code AS STRING))                                                                                                     
            )
            AND LEFT (
                CAST(CAST(cld_num AS BIGINT) AS STRING),                                                                                                        -- check if leading number of called number similar to dial number
                LENGTH (CAST(cc.country_code AS STRING)) -                                                                                                      -- eg.  if length difference of clg_num and cld_num, "len_diff" is 2,
                (LENGTH (CAST(cdr.clg_num AS STRING)) - LENGTH (CAST(CAST(cld_num AS BIGINT) AS STRING)))                                                                                                                                               --      select first first(cld_num, dial number length - len_diff),
            ) = RIGHT (                                                                                                                                         --      compare if it is the same as last(dial_no, dial number length - len_diff)
                CAST(cc.country_code AS STRING),
                LENGTH (CAST(cc.country_code AS STRING)) - (LENGTH (CAST(cdr.clg_num AS STRING)) - LENGTH (CAST(CAST(cld_num AS BIGINT) AS STRING)))
            )                                                                                                                                                   -- only if called number follow msisdn standard (limited to 15 digits)
            AND LEFT (
                CAST(cdr.clg_num AS STRING),
                LENGTH (CAST(cc.country_code AS STRING))
            ) = CAST(cc.country_code AS STRING)                                                                                                                 -- check if calling number start with their own dial number (eg. Singapore caller must start with +65) 
            AND LENGTH (CAST(cc.country_code AS STRING)) >= (                                                                                                   -- check if dial number's length greater or equal than the length difference of clg_num and cld_num
                LENGTH (CAST(cdr.clg_num AS STRING)) - LENGTH (CAST(CAST(cld_num AS BIGINT) AS STRING))
            )  
            AND SUBSTR(
                CAST(CAST(cld_num AS BIGINT) AS STRING),
                LENGTH (CAST(cdr.clg_num AS STRING)) - LENGTH (CAST(CAST(cld_num AS BIGINT) AS STRING)) + 1,
                LENGTH(ndc.ndc_code)
            ) in (SELECT ndc_code FROM suggest_ndc)
        GROUP BY
            cdr.clg_mcc_ref,
            cc.country_code,
            cdr.clg_num,
            ndc.ndc_code,
            CAST(CAST(cld_num AS BIGINT) AS STRING)
    ) 


SELECT * FROM base_tbl 