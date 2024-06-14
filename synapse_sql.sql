SELECT TOP (100) [customer_id]
,[balance]
,[products_number]
,[credit_card]
,[active_member]
,[estimated_salary]
,[churn]
,[credit_score]
,[country]
,[gender]
,[age]
,[tenure]
 FROM [churn_db].[dbo].[churn]





-- Customer Churn by Tenure and Products
SELECT
    CASE
        WHEN tenure <= 2 THEN '0-2 years'
        WHEN tenure > 2 AND tenure <= 5 THEN '3-5 years'
        WHEN tenure > 5 AND tenure <= 10 THEN '6-10 years'
        ELSE 'Over 10 years'
    END AS tenure_range,
    products_number,
    CAST(SUM(churn) AS FLOAT) as total_churns, COUNT(*) as total_customers, ROUND((CAST(SUM(churn) AS FLOAT)/COUNT(*))*100,2) as churn_rate_percentage
FROM
    dbo.churn
GROUP BY
    CASE
        WHEN tenure <= 2 THEN '0-2 years'
        WHEN tenure > 2 AND tenure <= 5 THEN '3-5 years'
        WHEN tenure > 5 AND tenure <= 10 THEN '6-10 years'
        ELSE 'Over 10 years'
    END,
    products_number
ORDER BY churn_rate_percentage DESC;







-- Calculate churn proportion by number of products
SELECT
    products_number,
    (ROUND(CAST(SUM(churn) AS FLOAT) / COUNT(*),2)*100) AS churn_proportion

FROM
    dbo.churn
GROUP BY
    products_number
ORDER BY 
    churn_proportion DESC





--churn by country
SELECT
    country,
    (ROUND(CAST(SUM(churn) AS FLOAT) / COUNT(*),2)*100) AS churn_proportion
FROM
    churn
GROUP BY
    country;






--churn by gender
SELECT
    gender, ROUND((CAST(SUM(churn) AS FLOAT)/COUNT(*))*100,2) AS churn_proportion
FROM
    churn
WHERE country = 'Germany'
GROUP BY
    gender;
