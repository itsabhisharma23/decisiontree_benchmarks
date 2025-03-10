SELECT Cast(amc AS NUMERIC) / Cast(pmc AS NUMERIC) 
               am_pm_ratio 
FROM   (SELECT Count(*) amc 
        FROM   `hadoop-benchmarking01`.TPCDS_1TB.web_sales, 
               `hadoop-benchmarking01`.TPCDS_1TB.household_demographics, 
               `hadoop-benchmarking01`.TPCDS_1TB.time_dim, 
               `hadoop-benchmarking01`.TPCDS_1TB.web_page 
        WHERE  ws_sold_time_sk = time_dim.t_time_sk 
               AND ws_ship_hdemo_sk = household_demographics.hd_demo_sk 
               AND ws_web_page_sk = web_page.wp_web_page_sk 
               AND time_dim.t_hour BETWEEN 12 AND 12 + 1 
               AND household_demographics.hd_dep_count = 8 
               AND web_page.wp_char_count BETWEEN 5000 AND 5200) at1, 
       (SELECT Count(*) pmc 
        FROM   `hadoop-benchmarking01`.TPCDS_1TB.web_sales, 
               `hadoop-benchmarking01`.TPCDS_1TB.household_demographics, 
               `hadoop-benchmarking01`.TPCDS_1TB.time_dim, 
               `hadoop-benchmarking01`.TPCDS_1TB.web_page 
        WHERE  ws_sold_time_sk = time_dim.t_time_sk 
               AND ws_ship_hdemo_sk = household_demographics.hd_demo_sk 
               AND ws_web_page_sk = web_page.wp_web_page_sk 
               AND time_dim.t_hour BETWEEN 20 AND 20 + 1 
               AND household_demographics.hd_dep_count = 8 
               AND web_page.wp_char_count BETWEEN 5000 AND 5200) pt 
ORDER  BY am_pm_ratio;