
--Weekly Average Listing Price
DROP TABLE IF EXISTS presentation.avg_listing_price;

CREATE TABLE presentation.avg_listing_price AS
SELECT
    DATE_TRUNC('week', listing_created_on) AS week,
    AVG(CAST(price_double AS FLOAT)) AS avg_price
FROM curated.apartments
WHERE listing_created_on IS NOT NULL AND price IS NOT NULL
GROUP BY 1;

--Monthly Occupancy Rate
DROP TABLE IF EXISTS presentation.occupancy_rate;

CREATE TABLE presentation.occupancy_rate AS
SELECT
    DATE_TRUNC('month', checkin_date) AS month,
    (COUNT(*)::decimal / NULLIF(SUM(DATEDIFF(day, checkin_date, checkout_date)), 0)) * 100 AS occupancy_pct
FROM curated.bookings
WHERE checkin_date IS NOT NULL AND checkout_date IS NOT NULL
GROUP BY 1;


--Repeat Customer Rate
DROP TABLE IF EXISTS presentation.repeat_customers;

CREATE TABLE presentation.repeat_customers AS
WITH activity AS (
    SELECT user_id,
           COUNT(*) AS total_bookings
    FROM curated.bookings
    GROUP BY user_id
)
SELECT
    CASE WHEN COUNT(*) = 0 THEN 0.0
         ELSE SUM(CASE WHEN total_bookings > 1 THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100
    END AS repeat_pct
FROM activity;







DROP TABLE IF EXISTS presentation.occupancy_rate;

CREATE TABLE presentation.occupancy_rate AS
SELECT
    DATE_TRUNC('month', checkin_date) AS month,
    COUNT(*)::decimal / SUM(DATEDIFF(day, checkin_date, checkout_date)) * 100 AS occupancy_pct
FROM curated.bookings
WHERE checkin_date IS NOT NULL AND checkout_date IS NOT NULL
GROUP BY 1;




DROP TABLE IF EXISTS presentation.repeat_customers;

CREATE TABLE presentation.repeat_customers AS
WITH activity AS (
    SELECT user_id,
           COUNT(*) AS total_bookings
    FROM curated.bookings
    GROUP BY user_id
)
SELECT
    CASE WHEN COUNT(*) = 0 THEN 0.0
         ELSE SUM(CASE WHEN total_bookings > 1 THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100
    END AS repeat_pct
FROM activity;