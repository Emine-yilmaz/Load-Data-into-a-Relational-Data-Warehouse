# Load-Data-into-a-Relational-Data-Warehouse
# Azure Synapse Analytics: Load Data into a Relational Data Warehouse

This project demonstrates how to load data into a dedicated SQL pool in Azure Synapse Analytics using staging tables, the COPY statement, CTAS queries, and slowly changing dimension logic.

> **Estimated time to complete**: ~30 minutes  
> **Prerequisites**: An active Azure subscription with Contributor or Owner access.

---

## 🚀 Project Overview

In this exercise, you'll:

- Provision an Azure Synapse Analytics workspace
- Load CSV data from Azure Data Lake into staging tables
- Transform and load data into dimension tables using CTAS and SCD (Slowly Changing Dimension) logic
- Optimize your data warehouse for query performance

---

## 🛠️ Setup Instructions

### 1. Clone the Repository

Open [Azure Portal](https://portal.azure.com), launch **Cloud Shell** using PowerShell, and run:

```powershell
rm -r dp-203 -f
git clone https://github.com/MicrosoftLearning/dp-203-azure-data-engineer dp-203
cd dp-203/Allfiles/labs/09
./setup.ps1
```
Follow the prompts to select your subscription and set a password for the Synapse SQL pool.

🔐 Remember your SQL pool password. You’ll need it later!

### 2. Open Synapse Studio
-Go to the resource group (e.g., dp203-xxxxxx) in Azure Portal
-Select your Synapse workspace
-Click Open Synapse Studio

### 3. Verify Data Lake Files
-In Synapse Studio:
-Go to Data > Linked > Expand your Data Lake (e.g., datalakexxxxx)
-Navigate to files/data/ and verify the presence of Customer.csv and Product.csv

## 📊 Load Data into Staging Tables
Open a new SQL Script connected to your SQL pool and run:

### Load Products
```sql
COPY INTO dbo.StageProduct
    (ProductID, ProductName, ProductCategory, Color, Size, ListPrice, Discontinued)
FROM 'https://<your-datalake>.blob.core.windows.net/files/data/Product.csv'
WITH (
    FILE_TYPE = 'CSV',
    FIRSTROW = 2,
    MAXERRORS = 0,
    IDENTITY_INSERT = 'OFF'
);
```
### Load Customers (with error handling)

```sql
COPY INTO dbo.StageCustomer
(
    GeographyKey, CustomerAlternateKey, Title, FirstName, MiddleName, LastName, NameStyle, BirthDate,
    MaritalStatus, Suffix, Gender, EmailAddress, YearlyIncome, TotalChildren, NumberChildrenAtHome,
    EnglishEducation, SpanishEducation, FrenchEducation, EnglishOccupation, SpanishOccupation,
    FrenchOccupation, HouseOwnerFlag, NumberCarsOwned, AddressLine1, AddressLine2, Phone,
    DateFirstPurchase, CommuteDistance
)
FROM 'https://<your-datalake>.dfs.core.windows.net/files/data/Customer.csv'
WITH (
    FILE_TYPE = 'CSV',
    FIRSTROW = 2,
    MAXERRORS = 5,
    ERRORFILE = 'https://<your-datalake>.dfs.core.windows.net/files/'
);
```
## 🏗️ Create Dimension Tables

### Create DimProduct with CTAS

```sql
CREATE TABLE dbo.DimProduct
WITH (
    DISTRIBUTION = HASH(ProductAltKey),
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT
    ROW_NUMBER() OVER(ORDER BY ProductID) AS ProductKey,
    ProductID AS ProductAltKey,
    ProductName, ProductCategory, Color, Size, ListPrice, Discontinued
FROM dbo.StageProduct;
```
## 🔁 Load Slowly Changing Dimension (SCD) Data

```sql
-- Insert new customers
INSERT INTO dbo.DimCustomer (...)
SELECT * FROM dbo.StageCustomer AS stg
WHERE NOT EXISTS (
    SELECT 1 FROM dbo.DimCustomer AS dim
    WHERE dim.CustomerAlternateKey = stg.CustomerAlternateKey
);

-- Type 1: Overwrite name/email/phone
UPDATE dbo.DimCustomer
SET LastName = stg.LastName, EmailAddress = stg.EmailAddress, Phone = stg.Phone
FROM dbo.DimCustomer dim
JOIN dbo.StageCustomer stg ON dim.CustomerAlternateKey = stg.CustomerAlternateKey
WHERE dim.LastName <> stg.LastName OR dim.EmailAddress <> stg.EmailAddress OR dim.Phone <> stg.Phone;

-- Type 2: Insert if address changed
INSERT INTO dbo.DimCustomer (...)
SELECT stg.* FROM dbo.StageCustomer stg
JOIN dbo.DimCustomer dim ON stg.CustomerAlternateKey = dim.CustomerAlternateKey
WHERE stg.AddressLine1 <> dim.AddressLine1;
```

## ⚙️ Optimize Performance

### Rebuild Index

```sql
ALTER INDEX ALL ON dbo.DimProduct REBUILD;
```

### Create Statistics

```sql
CREATE STATISTICS customergeo_stats ON dbo.DimCustomer (GeographyKey);
```

## 📌 Notes
-You can preview rejected rows in the _rejectedrows/ folder inside your Data Lake.
-Use CTAS to improve performance and assign surrogate keys.
-Use SCD logic to handle historical changes in dimension data.

## 🧠 Resources

-Azure Synapse Analytics Documentation
-Data loading strategies for dedicated SQL pool
