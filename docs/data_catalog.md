# Data Catalog for Gold Layer

## Overview
The Gold Layer represents business-level data, organized to support analytics and reporting. It includes dimension tables and fact tables that capture key business metrics.

---

### 1. **gold.dim_customers**
- Purpose: Contains customer information enhanced with demographic and geographic attributes.
- Columns:

| Column Name      | Data Type     | Description                                                                                   |
|------------------|---------------|-----------------------------------------------------------------------------------------------|
| customer_key     | INT           | A surrogate key that uniquely identifies each customer entry in the dimension table.          |
| customer_id      | INT           | A system-generated numeric ID assigned to each customer.                                      |
| customer_number  | NVARCHAR(50)  | A unique alphanumeric code used to identify and reference the customer.                       |
| first_name       | NVARCHAR(50)  | The first name of the customer, as stored in the system.                                      |
| last_name        | NVARCHAR(50)  | The last name of the customer, as stored in the system.                                       |
| country          | NVARCHAR(50)  | The customer's country of residence (e.g., 'Australia').                                      |
| marital_status   | NVARCHAR(50)  | Indicates the customer’s marital status (e.g., 'Married', 'Single').                          |
| gender           | NVARCHAR(50)  | The customer’s gender information (e.g., 'Male', 'Female', or 'n/a').                         |
| birthdate        | DATE          | The customer’s birth date in the format YYYY-MM-DD (e.g., 1971-10-06).                        |
| create_date      | DATE          | The timestamp representing when the customer record was added to the system.                  |

---

### 2. **gold.dim_products**
- Purpose: Contains details about products along with their associated attributes.
- Columns:

| Column Name         | Data Type     | Description                                                                                      |
|---------------------|---------------|--------------------------------------------------------------------------------------------------|
| product_key         | INT           | Surrogate key that uniquely identifies each product record in the dimension table.               |
| product_id          | INT           | Unique system-assigned identifier used to track and reference each product.                      |
| product_number      | NVARCHAR(50)  | Alphanumeric code used to represent the product, often for categorization or inventory purposes. |
| product_name        | NVARCHAR(50)  | Name of the product, typically including attributes like type, color, or size.                   |
| category_id         | NVARCHAR(50)  | Unique code that identifies the product's category, linking it to a broader classification.      |
| category            | NVARCHAR(50)  | The broader classification of the product (e.g., Bikes, Components) to group related items.      |
| subcategory         | NVARCHAR(50)  | A more specific classification within the product’s category, detailing its type.                |
| maintenance_required| NVARCHAR(50)  | Specifies if the product needs maintenance (e.g., 'Yes', 'No').                                  |
| cost                | INT           | The base price or cost associated with the product, in monetary terms.                           |
| product_line        | NVARCHAR(50)  | The product series or range it belongs to (e.g., Road, Mountain).                                |
| start_date          | DATE          | The date the product became available for sale or use.                                           |

---

### 3. **gold.fact_sales**
- Purpose: Contains transactional sales records used for analysis and reporting.
- Columns:

| Column Name     | Data Type     | Description                                                                                   |
|-----------------|---------------|-----------------------------------------------------------------------------------------------|
| order_number    | NVARCHAR(50)  | Unique alphanumeric code identifying each sales order (e.g., 'SO54496').                      |
| product_key     | INT           | Foreign key referencing the associated product in the product dimension table.                |
| customer_key    | INT           | Foreign key referencing the related customer in the customer dimension table.                 |
| order_date      | DATE          | Date on which the sales order was created.                                                    |
| shipping_date   | DATE          | Date on which the order was shipped to the customer.                                          |
| due_date        | DATE          | Date by which the payment for the order was expected.                                         |
| sales_amount    | INT           | Total value of the sale for the line item, in whole currency units (e.g., 25).                |
| quantity        | INT           | Number of product units ordered in the transaction line (e.g., 1).                            |
| price           | INT           | Unit price of the product for the line item, in whole currency units (e.g., 25).              |

