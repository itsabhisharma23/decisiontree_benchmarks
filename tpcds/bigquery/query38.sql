SELECT Count(*) 
FROM   (SELECT DISTINCT c_last_name, 
                        c_first_name, 
                        d_date 
        FROM   `hadoop-benchmarking01`.TPCDS_1TB.store_sales, 
               `hadoop-benchmarking01`.TPCDS_1TB.date_dim, 
               `hadoop-benchmarking01`.TPCDS_1TB.customer 
        WHERE  store_sales.ss_sold_date_sk = date_dim.d_date_sk 
               AND store_sales.ss_customer_sk = customer.c_customer_sk 
               AND d_month_seq BETWEEN 1188 AND 1188 + 11 
        INTERSECT DISTINCT 
        SELECT DISTINCT c_last_name, 
                        c_first_name, 
                        d_date 
        FROM   `hadoop-benchmarking01`.TPCDS_1TB.catalog_sales, 
               `hadoop-benchmarking01`.TPCDS_1TB.date_dim, 
               `hadoop-benchmarking01`.TPCDS_1TB.customer 
        WHERE  catalog_sales.cs_sold_date_sk = date_dim.d_date_sk 
               AND catalog_sales.cs_bill_customer_sk = customer.c_customer_sk 
               AND d_month_seq BETWEEN 1188 AND 1188 + 11 
        INTERSECT DISTINCT
        SELECT DISTINCT c_last_name, 
                        c_first_name, 
                        d_date 
        FROM   `hadoop-benchmarking01`.TPCDS_1TB.web_sales, 
               `hadoop-benchmarking01`.TPCDS_1TB.date_dim, 
               `hadoop-benchmarking01`.TPCDS_1TB.customer 
        WHERE  web_sales.ws_sold_date_sk = date_dim.d_date_sk 
               AND web_sales.ws_bill_customer_sk = customer.c_customer_sk 
               AND d_month_seq BETWEEN 1188 AND 1188 + 11) hot_cust;