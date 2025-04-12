---
layout: post
title: "Task 3: MySQL Queries and LLM Testing"
date: 2025-04-12 11:00:00 +0800
categories: 数据分析
---



## 任务三：MySQL查询与 LLM 测试报告

### 任务概述
任务三的目标是使用 MySQL 导入电信数据集后，对数据提出问题并解答，然后使用大语言模型（LLM）生成对应的 SQL 语句，分析 LLM 在处理 SQL 任务中的表现和问题。电信数据集包含客户信息，如客户 ID、使用时长（`tenure`）、是否流失（`Churn`）、月费（`MonthlyCharges`）和合同类型（`Contract`）。我设计了两个案例，分别分析续约次数和合同变更对流失率的影响。

### 数据导入
我首先将电信数据集导入 MySQL，创建了 `telco_data` 表，结构如下：

```sql
CREATE TABLE telco_data (
    customerID VARCHAR(20) PRIMARY KEY,
    tenure INT,
    Churn VARCHAR(10),
    MonthlyCharges FLOAT,
    Contract VARCHAR(20)
);
```

数据导入后，我提出了两个问题，并尝试使用 LLM（Qwen2.5 Max）生成 SQL 解答。

### 案例 1：续约记录与流失趋势

#### 问题
1. 生成一个新表 `renewal_records`，记录客户的续约信息：
   - `customerID`：客户 ID。
   - `renewal_count`：续约次数（`tenure` 除以 12 的整数部分）。
   - `last_renewal_tenure`：最后一次续约时的 `tenure`（`renewal_count * 12`）。
2. 分析续约次数对流失率的影响：
   - 计算每种续约次数的流失率（`churn_rate`）和平均月费（`avg_monthly_charges`）。
   - 按流失率降序排列。

#### 我的解答
我使用 MySQL 创建了 `renewal_records` 表，并计算了流失率和平均月费。SQL 语句如下：

```sql
-- 创建 renewal_records 表
DROP TABLE IF EXISTS renewal_records;
CREATE TABLE renewal_records (
    customerID VARCHAR(20) PRIMARY KEY,
    renewal_count INT,
    last_renewal_tenure INT
);

INSERT INTO renewal_records (customerID, renewal_count, last_renewal_tenure)
SELECT 
    customerID,
    FLOOR(tenure / 12) AS renewal_count,
    FLOOR(tenure / 12) * 12 AS last_renewal_tenure
FROM telco_data;

-- 分析续约次数对流失率的影响
SELECT 
    r.renewal_count,
    ROUND(SUM(CASE WHEN t.Churn = 'Yes' THEN 1 ELSE 0 END) / COUNT(*), 4) AS churn_rate,
    ROUND(AVG(t.MonthlyCharges), 2) AS avg_monthly_charges
FROM renewal_records r
JOIN telco_data t ON r.customerID = t.customerID
GROUP BY r.renewal_count
ORDER BY churn_rate DESC;
```

**结果**：

| renewal_count | churn_rate | avg_monthly_charges |
|---------------|------------|---------------------|
| 0   | 0.4828| 56.06|
| 1   | 0.2951| 60.85|
| 2   | 0.2203| 65.13|
| 3   | 0.1952| 66.43|
| 4   | 0.15  | 69.74|
| 5   | 0.083 | 74.3 |
| 6   | 0.0166| 80.7 |

**分析**：续约次数越多，流失率越低，但平均月费逐渐增加，说明长期续约的客户更稳定，但可能需要支付更高的费用。

#### LLM 测试
我将问题提交给 Qwen2.5 Max，Prompt 如下：

```
你是一个智能代理，负责根据以下信息生成正确的 SQL 语句：
- 数据库结构信息：包括表名、字段、表之间的关系（如外键等）。
- 用户问题：自然语言查询或问题。
你的主要任务是：
1. 分析数据库结构信息：
- 根据数据库结构信息，理解表的字段和关系，构建 SQL 语句的基本框架。
2. 生成 SQL 语句：
- 根据用户问题、查询要求和条件、涉及的表以及辅助查询条件，构建完整的 SQL 语句。
3. 验证和优化：
- 检查生成的 SQL 语句是否逻辑正确，并在必要时进行优化。
### 输入：
- 数据库结构信息：
  CREATE TABLE telco_data (
 customerID VARCHAR(20) PRIMARY KEY,
 tenure INT,
 Churn VARCHAR(10),
 MonthlyCharges FLOAT,
 Contract VARCHAR(20)
  );
- 用户问题：
  1. 基于 telco_data 表，生成一个新的表 renewal_records，记录每个客户的续约信息。假设每个客户在 tenure 达到 12 个月的倍数时会续约（例如 tenure 为 24 个月时续约 2 次）。renewal_records 表应包含以下字段：
- customerID（客户 ID，与 telco_data 的 customerID 对应）
- renewal_count（续约次数，tenure 除以 12 的整数部分）
- last_renewal_tenure（最后一次续约时的 tenure，即 renewal_count * 12）
  2. 使用 telco_data 和 renewal_records 表，分析续约次数对流失率的影响：
- 计算每种续约次数（renewal_count）的流失率（流失客户数占该续约次数客户的总数的比例）。
- 同时，计算每种续约次数下客户的平均 MonthlyCharges。
- 按流失率降序排列，结果应包含 renewal_count, 流失率（命名为 churn_rate），和平均 MonthlyCharges（命名为 avg_monthly_charges）。
### 输出：
- 以 JSON 格式返回结果，格式为 {"sql": "包含所有步骤的完整 SQL 语句"}
```

**Qwen2.5 Max 生成的 SQL**：

```json
{ "sql": " -- Step 1: 创建 renewal_records 表并插入数据 CREATE TABLE renewal_records AS SELECT customerID, FLOOR(tenure / 12) AS renewal_count, FLOOR(tenure / 12) * 12 AS last_renewal_tenure FROM telco_data;

-- Step 2: 分析续约次数对流失率的影响 SELECT r.renewal_count, ROUND(SUM(CASE WHEN t.Churn = 'Yes' THEN 1 ELSE 0 END) / COUNT(*)::NUMERIC, 4) AS churn_rate, ROUND(AVG(t.MonthlyCharges), 2) AS avg_monthly_charges FROM renewal_records r JOIN telco_data t ON r.customerID = t.customerID GROUP BY r.renewal_count ORDER BY churn_rate DESC; " }
```

**分析 LLM 生成结果的问题**：
- **问题 1：表创建方式不正确**：
  - Qwen2.5 Max 使用了 `CREATE TABLE AS SELECT`，这种方式在 MySQL 中虽然可行，但没有定义字段类型（例如 `customerID` 应为 `VARCHAR(20)`），可能导致数据类型不一致。
  - 正确的做法是先创建表并定义字段类型，然后使用 `INSERT INTO ... SELECT` 插入数据。
- **问题 2：未处理主键约束**：
  - `renewal_records` 表需要将 `customerID` 设置为主键，Qwen2.5 Max 忽略了这一点，可能导致数据插入时出现问题。
- **问题 3：语法细节**：
  - 在 `COUNT(*)::NUMERIC` 中使用了 `::NUMERIC`，这是 PostgreSQL 的语法，在 MySQL 中不适用。MySQL 中可以直接使用 `COUNT(*)`。

### 案例 2：合同变更与流失风险

#### 问题
1. 生成一个新表 `contract_changes`，记录客户的合同变更信息：
   - 如果 `tenure < 24`，则无变更，`previous_contract` 和 `current_contract` 都等于 `Contract`。
   - 如果 `tenure >= 24`：
- `Month-to-month` 变更为 `One year`。
- `One year` 变更为 `Two year`。
- `Two year` 变更为 `Month-to-month`。
   - 表字段包括 `customerID`、`previous_contract`、`current_contract` 和 `change_tenure`（无变更时为 0，变更时为 24）。
2. 分析合同变更对流失风险的影响：
   - 计算每种合同变更组合的流失率（`churn_rate`）和平均 `tenure`（`avg_tenure`）。
   - 按流失率降序排列。

#### 我的解答
我使用 MySQL 创建了 `contract_changes` 表，并计算了流失率和平均 `tenure`。SQL 语句如下：

```sql
-- 创建 contract_changes 表
DROP TABLE IF EXISTS contract_changes;
CREATE TABLE contract_changes (
    customerID VARCHAR(20) PRIMARY KEY,
    previous_contract VARCHAR(20),
    current_contract VARCHAR(20),
    change_tenure INT
);

INSERT INTO contract_changes (customerID, previous_contract, current_contract, change_tenure)
SELECT 
    customerID,
    Contract AS previous_contract,
    CASE 
   WHEN tenure < 24 THEN Contract
   WHEN Contract = 'Month-to-month' THEN 'One year'
   WHEN Contract = 'One year' THEN 'Two year'
   WHEN Contract = 'Two year' THEN 'Month-to-month'
    END AS current_contract,
    CASE 
   WHEN tenure >= 24 THEN 24 
   ELSE 0 
    END AS change_tenure
FROM telco_data;

-- 分析合同变更对流失风险的影响
SELECT 
    c.previous_contract,
    c.current_contract,
    ROUND(SUM(CASE WHEN t.Churn = 'Yes' THEN 1 ELSE 0 END) / COUNT(*), 4) AS churn_rate,
    ROUND(AVG(t.tenure), 2) AS avg_tenure
FROM contract_changes c
JOIN telco_data t ON c.customerID = t.customerID
GROUP BY c.previous_contract, c.current_contract
ORDER BY churn_rate DESC;
```

**结果**：

| previous_contract | current_contract | churn_rate | avg_tenure |
|-------------------|------------------|------------|------------|
| Month-to-month    | Month-to-month   | 0.4785| 7.81  |
| Month-to-month    | One year    | 0.3128| 40.79 |
| One year| Two year    | 0.1169| 49.23 |
| One year| One year    | 0.0963| 14.07 |
| Two year| Month-to-month   | 0.0309| 60.78 |
| Two year| Two year    | 0.0   | 12.54 |

**分析**：合同从“Month-to-month”变更为“One year”的客户流失率较低，而“Two year”变更为“Month-to-month”的客户流失率较高，说明长期合同对降低流失率有帮助。

#### LLM 测试
我将问题提交给 Qwen2.5 Max，Prompt 如下：

```
你是一个智能代理，负责根据以下信息生成正确的 SQL 语句：
- 数据库结构信息：包括表名、字段、表之间的关系（如外键等）。
- 用户问题：自然语言查询或问题。
你的主要任务是：
1. 分析数据库结构信息：
- 根据数据库结构信息，理解表的字段和关系，构建 SQL 语句的基本框架。
2. 生成 SQL 语句：
- 根据用户问题、查询要求和条件、涉及的表以及辅助查询条件，构建完整的 SQL 语句。
3. 验证和优化：
- 检查生成的 SQL 语句是否逻辑正确，并在必要时进行优化。
### 输入：
- 数据库结构信息：
  CREATE TABLE telco_data (
 customerID VARCHAR(20) PRIMARY KEY,
 tenure INT,
 Churn VARCHAR(10),
 MonthlyCharges FLOAT,
 Contract VARCHAR(20)
  );
- 用户问题：
  1. 假设客户可能在 tenure 达到 24 个月时变更合同类型，生成一个新表 contract_changes，记录客户的合同变更信息：
- 如果客户的 tenure 小于 24 个月，则没有变更，previous_contract 和 current_contract 都等于 Contract。
- 如果客户的 tenure 大于或等于 24 个月，假设他们将合同变更为：
  - 如果原合同是“Month-to-month”，则变更为“One year”。
  - 如果原合同是“One year”，则变更为“Two year”。
  - 如果原合同是“Two year”，则变更为“Month-to-month”。
- contract_changes 表应包含以下字段：
  - customerID（客户 ID）
  - previous_contract（变更前的合同类型）
  - current_contract（变更后的合同类型）
  - change_tenure（变更时的 tenure，如果没有变更则为 0，否则为 24）
  2. 使用 telco_data 和 contract_changes 表，分析合同变更对流失风险的影响：
- 计算每种合同变更组合（previous_contract 到 current_contract）的流失率。
- 同时，计算每种变更组合下客户的平均 tenure。
- 按流失率降序排列，结果应包含 previous_contract, current_contract, 流失率（命名为 churn_rate），和平均 tenure（命名为 avg_tenure）。
### 输出：
- 以 JSON 格式返回结果，格式为 {"sql": "包含所有步骤的完整 SQL 语句"}
```

**Qwen2.5 Max 生成的 SQL**：

```json
{ "sql": " -- Step 1: 创建 contract_changes 表并插入数据 CREATE TABLE contract_changes AS SELECT customerID, Contract AS previous_contract, CASE WHEN tenure < 24 THEN Contract WHEN Contract = 'Month-to-month' THEN 'One year' WHEN Contract = 'One year' THEN 'Two year' WHEN Contract = 'Two year' THEN 'Month-to-month' END AS current_contract, CASE WHEN tenure >= 24 THEN 24 ELSE 0 END AS change_tenure FROM telco_data;

-- Step 2: 分析合同变更对流失风险的影响 SELECT cc.previous_contract, cc.current_contract, ROUND(SUM(CASE WHEN t.Churn = 'Yes' THEN 1 ELSE 0 END) / COUNT(*)::NUMERIC, 4) AS churn_rate, ROUND(AVG(t.tenure), 2) AS avg_tenure FROM contract_changes cc JOIN telco_data t ON cc.customerID = t.customerID GROUP BY cc.previous_contract, cc.current_contract ORDER BY churn_rate DESC; " }
```

**分析 LLM 生成结果的问题**：
- **问题 1：表创建方式不正确**：
  - 与案例 1 类似，Qwen2.5 Max 使用了 `CREATE TABLE AS SELECT`，未定义字段类型，可能导致数据类型不一致。
  - 正确的做法是先创建表并定义字段类型，然后插入数据。
- **问题 2：未处理主键约束**：
  - `contract_changes` 表需要将 `customerID` 设置为主键，Qwen2.5 Max 忽略了这一点。
- **问题 3：语法细节**：
  - 同样使用了 `COUNT(*)::NUMERIC`，这是 PostgreSQL 语法，在 MySQL 中不适用。

### LLM 表现总结
Qwen2.5 Max 在生成 SQL 语句时存在以下问题：
- **技术约束理解不足**：未正确处理 MySQL 的表创建语法，忽略了字段类型和主键约束。
- **语法兼容性问题**：使用了 PostgreSQL 的语法（`::NUMERIC`），不适用于 MySQL。
- **逻辑细节缺失**：未充分考虑数据类型和计算逻辑的正确性。

相比之下，我的解答更注重 MySQL 的语法兼容性和表结构的完整性，确保了 SQL 语句的正确性和可执行性。

### 结论
任务三通过 MySQL 对电信数据集进行了复杂查询，分析了续约次数和合同变更对客户流失的影响。LLM（Qwen2.5 Max）在生成 SQL 语句时表现出一定的逻辑能力（提到的两个例子是用来测试的好多个例子中出现错误的，其他相对简单任务LLM在处理时不容易出现问题），但缺乏对数据库技术约束和语法兼容性的理解。