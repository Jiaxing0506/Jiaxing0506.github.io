---
layout: post
title: "Task 3: Complex Queries and LLM Testing Report"
date: 2025-04-12 11:00:00 +0800
categories: data-analysis
---

## Task 3: Complex Queries and LLM Testing Report

### Overview
Task 3 involved performing complex data analysis using PySpark and MySQL, focusing on customer churn in a telecommunications dataset. The task was divided into two complex cases:
- **Case 1**: Generate a renewal records table and analyze the impact of renewal frequency on churn rates.
- **Case 2**: Generate a contract changes table and analyze the impact of contract changes on churn risk.

Additionally, the task required testing a large language model (LLM) to generate SQL queries and analyzing its performance.

### Case 1: Renewal Records and Churn Analysis
I created a new table `renewal_records` in MySQL to store customer renewal information:
- `customerID`: Customer identifier.
- `renewal_count`: Number of renewals (calculated as `FLOOR(tenure / 12)`).
- `last_renewal_tenure`: Tenure at the last renewal (calculated as `FLOOR(tenure / 12) * 12`).

I then used PySpark to query the data and calculate the churn rate and average monthly charges for each renewal count. The SQL query was:

```sql
SELECT 
    r.renewal_count,
    ROUND(SUM(CASE WHEN t.Churn = 'Yes' THEN 1 ELSE 0 END) / COUNT(*), 4) as churn_rate,
    ROUND(AVG(t.MonthlyCharges), 2) as avg_monthly_charges
FROM renewal_records r
JOIN telco_data t ON r.customerID = t.customerID
GROUP BY r.renewal_count
ORDER BY churn_rate DESC;