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



-- most popular design_name for each country (or city if there are minimal countries)
-- biggest spend (units sold * sales price) per city
-- for each each currency name, top 3 most popular design name
-- bottom 3 counterparty legal names by country
