SELECT a.ca_state state, 
               Count(*)   cnt 
FROM   `hadoop-benchmarking01`.TPCDS_1TB.customer_address a, 
       `hadoop-benchmarking01`.TPCDS_1TB.customer c, 
       `hadoop-benchmarking01`.TPCDS_1TB.store_sales s, 
       `hadoop-benchmarking01`.TPCDS_1TB.date_dim d, 
       `hadoop-benchmarking01`.TPCDS_1TB.item i 
WHERE  a.ca_address_sk = c.c_current_addr_sk 
       AND c.c_customer_sk = s.ss_customer_sk 
       AND s.ss_sold_date_sk = d.d_date_sk 
       AND s.ss_item_sk = i.i_item_sk 
       AND d.d_month_seq = (SELECT DISTINCT ( d_month_seq ) 
                            FROM   `hadoop-benchmarking01`.TPCDS_1TB.date_dim 
                            WHERE  d_year = 1998 
                                   AND d_moy = 7) 
       AND i.i_current_price > 1.2 * (SELECT Avg(j.i_current_price) 
                                      FROM   `hadoop-benchmarking01`.TPCDS_1TB.item j 
                                      WHERE  j.i_category = i.i_category) 
GROUP  BY a.ca_state 
HAVING Count(*) >= 10 
ORDER  BY cnt;