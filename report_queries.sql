-- top 3 designs that sell the most units for each quarter of any year (desgin&sales&date)

-- SELECT
--     dim_date.quarter,
--     dim_design.design_name,
--     fact_sales_order.units_sold
--     RANK() OVER(
--         PARTITION BY dim_date.quarter
--         ORDER BY fact_sales_order.units_sold DESC
--         ) AS rank
-- FROM dim_date
-- JOIN fact_sales_order ON dim_date.date_id = fact_sales_order.created_date
-- JOIN dim_design ON dim_design.design_id = fact_sales_order.design_id

-- units sold * sales price for each day of the week (which day of the week would make the most sales)(date&sales)
SELECT dim_date.day_of_week, SUM(fact_sales_order.units_sold*fact_sales_order.unit_price) as total_sales
FROM dim_date  
JOIN fact_sales_order on dim_date.date_id = fact_sales_order.created_date
GROUP BY dim_date.day_of_week
ORDER BY day_of_week;

-- most popular design_name for each country (desgin&location&FACTSALES)
-- WITH PopularDesign as(
-- SELECT  d.design_name, l.country , SUM(fact_sales_order.units_sold) as total_units_sold
-- FROM fact_sales_order fs
-- JOIN dim_design d on fs.design_id=d.design_id
-- JOIN dim_location l on fs.agreed_delivery_location_id=l.location_id
-- GROUP BY d.design_name, l.country),
-- MaxTotalUntisSold As(
-- SELECT l.country, MAX(total_units_sold) as max_total_units_sold 
-- FROM PopularDesign
-- GROUP BY l.country)


--most popular design_name for each city if there are minimal countries)







-- biggest spend (units sold * sales price) per counterparty legal name (counterparty&sales)
SELECT SUM(fact_sales_order.units_sold*fact_sales_order.unit_price) as total_spend, dim_counterparty.counterparty_legal_name
FROM dim_counterparty
JOIN fact_sales_order on  fact_sales_order.counterparty_id = dim_counterparty.counterparty_id
GROUP BY dim_counterparty.counterparty_legal_name
ORDER BY total_spend DESC;



-- for each each currency name, total number of units sold
SELECT cu.currency_name, SUM(fs.units_sold) as total_units_sold
FROM fact_sales_order fs
JOIN dim_currency cu on fs.currency_id = cu.currency_id
GROUP BY cu.currency_name
ORDER BY total_units_sold DESC;




-- top 10 countries based units sold
SELECT 
DISTINCT l.country,
SUM(fs.units_sold) as total_units_sold
FROM fact_sales_order fs
JOIN dim_location l on fs.agreed_delivery_location_id=l.location_id
GROUP BY l.country
ORDER BY total_units_sold DESC LIMIT 10;