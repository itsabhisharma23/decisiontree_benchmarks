select count(*) 
from ((select distinct c_last_name, c_first_name, d_date
       from 
       `hadoop-benchmarking01`.TPCDS_1TB.store_sales, 
       `hadoop-benchmarking01`.TPCDS_1TB.date_dim, 
       `hadoop-benchmarking01`.TPCDS_1TB.customer
       where store_sales.ss_sold_date_sk = date_dim.d_date_sk
         and store_sales.ss_customer_sk = customer.c_customer_sk
         and d_month_seq between 1188 and 1188+11)
       except distinct
      (select distinct c_last_name, c_first_name, d_date
       from 
       `hadoop-benchmarking01`.TPCDS_1TB.catalog_sales, 
       `hadoop-benchmarking01`.TPCDS_1TB.date_dim, 
       `hadoop-benchmarking01`.TPCDS_1TB.customer
       where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
         and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
         and d_month_seq between 1188 and 1188+11)
       except distinct
      (select distinct c_last_name, c_first_name, d_date
       from 
       `hadoop-benchmarking01`.TPCDS_1TB.web_sales, 
       `hadoop-benchmarking01`.TPCDS_1TB.date_dim, 
       `hadoop-benchmarking01`.TPCDS_1TB.customer
       where web_sales.ws_sold_date_sk = date_dim.d_date_sk
         and web_sales.ws_bill_customer_sk = customer.c_customer_sk
         and d_month_seq between 1188 and 1188+11)
) cool_cust
;
