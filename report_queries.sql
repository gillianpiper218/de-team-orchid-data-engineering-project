-- top 3 designs that sell the most units for each quarter of any year

SELECT
    dim_date.quarter,
    dim_design.design_name,
    fact_sales_order.units_sold
    RANK() OVER(
        PARTITION BY dim_date.quarter
        ORDER BY fact_sales_order.units_sold DESC
        )
FROM dim_date
JOIN fact_sales_order ON dim_date.date_id = fact_sales_order.created_date
JOIN dim_design ON dim_design.design_id = fact_sales_order.design_id

-- units sold * sales price for each day of the week (which day of the week would make the most sales)
SELECT dim_date.day_of_week, SUM(fact_sales_order.units_sold*fact_sales_order.units_price) as total_sales
FROM dim_date  
JOIN fact_sales_order on dim_date.date_id = fact_sales_order.created_date
GROUP BY dim_date.day_of_week
ORDER BY total_sales DESC;

-- most popular design_name for each country (or city if there are minimal countries)
SELECT  dim_design.design_name
FROM dim_country





-- biggest spend (units sold * sales price) per city
SELECT SUM(fact_sales_order.units_sold*fact_sales_order.units_price) as total_spend, dim_country.counterparty_legal_name
FROM dim_country
JOIN fact_sales_order on  fact_sales_order.counterparty_id = dim_countrparty.counterparty_id
GROUP BY dim_countrparty.counterparty_legal_name
ORDER BY total_spend DESC



-- for each each currency name, top 3 most popular design name






-- bottom 3 counterparty legal names by country
