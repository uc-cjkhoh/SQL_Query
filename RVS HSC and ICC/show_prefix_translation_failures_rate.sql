-- Objection: For each country's failed/rejected call, find out the msisdn's prefix distribution in percentage or count 

WITH  
    -- Assuming the first 4 digits contain the msisdn's prefix, 
    -- Then, count the number of failed colling having the same 1st digit, 1st and 2nd digitss and so on
    base_tbl AS (
        SELECT 
            clg_mcc,
            clg_num,
            LENGTH(CAST(clg_num AS STRING)) AS clg_length,
            cld_num,
            LENGTH(CAST(cld_num AS STRING)) AS cld_length
        FROM 
            roam352_report.data_cdr 
        WHERE
            service_type = 301                                              -- ICC service
            AND status = '10001f'                                           -- rejected tx (ACM ?)
            AND par_bound_type = 1   
        GROUP BY 
            clg_mcc,
            clg_num,
            cld_num
        HAVING LENGTH(CAST(cld_num AS STRING)) <= 15
    ),
    -- Each countries traffic
    traffic AS (
        SELECT 
            clg_mcc,
            COUNT(*) AS traffic_count
        FROM 
            base_tbl
        GROUP BY 
            clg_mcc
    ),
    -- Each countries country code
    country_codes AS (
        SELECT
            mcc,
            MIN(LENGTH(CAST(country_code AS STRING))) as min_cc_len,
            MAX(LENGTH(CAST(country_code AS STRING))) as max_cc_len
        FROM
            roam352_report.sys_country_profile
        GROUP BY
            mcc 
    )
 

SELECT 
    tbl.clg_mcc,
    tbl.len_diff,
    cc.min_cc_len,
    cc.max_cc_len,
    tbl.total_count,
    100 * (tbl.total_count / t.traffic_count) as percentage
FROM (
    SELECT
        clg_mcc,
        clg_length - cld_length AS len_diff,
        COUNT(*) as total_count
    FROM 
        base_tbl
    WHERE 
        (clg_length - cld_length >= 0) 
        and (clg_length - cld_length <= 3) 
    GROUP BY
        clg_mcc,
        len_diff
) tbl 
JOIN traffic t ON (t.clg_mcc = tbl.clg_mcc)
JOIN country_codes cc ON (cc.mcc = tbl.clg_mcc)
ORDER BY
    tbl.clg_mcc,
    tbl.total_count
DESC




-- TESTING 
select 
    clg_num, cld_num 
from data_cdr 
where 
    service_type = 301                                              -- ICC service
    AND status = '10001f'                                           -- rejected tx (ACM ?)
    AND par_bound_type = 1 
    AND clg_mcc = 525
    AND LENGTH(cast(clg_num as string)) - LENGTH(cast(cld_num as string)) = 2