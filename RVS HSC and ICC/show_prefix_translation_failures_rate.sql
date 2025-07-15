-- Objection: For each country's failed/rejected call, find out the msisdn's prefix distribution in percentage or count 

WITH
    -- Count inbound ICC traffic, then select the top 10 countries with highest traffic
    target_countries AS (
        SELECT 
            cld_mcc_ref,
            COUNT(1) as traffic
        FROM
            roam352_report.data_cdr
        WHERE
            service_type = 301              -- ICC service
            -- AND status = '10001f'        -- rejected tx (ACM ?) 
            AND cld_mcc_ref != clg_mcc_ref  -- caller and callee not in same nation    
        GROUP BY
            cld_mcc_ref
        ORDER BY 
            traffic
        DESC LIMIT 10
    ),
    -- Assuming the first 4 digits contain the msisdn's prefix, 
    -- Then, count the number of failed colling having the same 1st digit, 1st and 2nd digitss and so on
    base_tbl AS (
        SELECT 
            cld_mcc_ref, 
            LEFT(cld_num, 1) as p1,
            LEFT(cld_num, 2) as p2,
            LEFT(cld_num, 3) as p3,
            LEFT(cld_num, 4) as p4
        FROM 
            roam352_report.data_cdr 
        WHERE
            service_type = 301                                              -- ICC service
            AND status = '10001f'                                           -- rejected tx (ACM ?) 
            AND cld_mcc_ref != clg_mcc_ref                                  -- caller and callee not in same nation    
            AND cld_mcc_ref in (SELECT cld_mcc_ref FROM target_countries)   -- target countries 
        GROUP BY 
            cld_mcc_ref,
            p1,
            p2,
            p3,
            p4
    ) 


SELECT * FROM (
    SELECT cld_mcc_ref, p1 as prefix, COUNT(*) as total_count FROM base_tbl GROUP BY cld_mcc_ref, p1
    UNION ALL
    SELECT cld_mcc_ref, p2 as prefix, COUNT(*) as total_count FROM base_tbl GROUP BY cld_mcc_ref, p2
    UNION ALL
    SELECT cld_mcc_ref, p3 as prefix, COUNT(*) as total_count FROM base_tbl GROUP BY cld_mcc_ref, p3
    UNION ALL
    SELECT cld_mcc_ref, p4 as prefix, COUNT(*) as total_count FROM base_tbl GROUP BY cld_mcc_ref, p4
) tbl
    WHERE total_count > 1
    ORDER BY cld_mcc_ref, total_count DESC