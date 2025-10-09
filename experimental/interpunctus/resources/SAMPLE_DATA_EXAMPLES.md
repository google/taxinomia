# Sample Data Examples

## Dataset Overview

The sample dataset contains 30 orders with the following dimensions:

**Categorical Columns:**
- `status`: Delivered, Shipped, Processing, Cancelled
- `priority`: High, Medium, Low
- `region`: North, South, East, West
- `country`: USA, Canada, Mexico
- `product_category`: Electronics, Furniture, Office Supplies
- `year`: 2024, 2025
- `quarter`: Q1, Q2, Q3, Q4
- `month`: January through January (13 months)

**Numeric Columns:**
- `amount`: Order value ($5 - $1400)
- `quantity`: Items ordered (1-50)

**Text Columns:**
- `order_id`: Unique identifier (ORD-001 to ORD-030)
- `customer`: Customer name
- `product`: Specific product name

## Example Use Cases

### 1. Filter by Status, Group by Region and Category

**Query:** Show only delivered orders, grouped by region then product category

**URL:**
```
?columns=status+,region+,product_category+,product+,amount+,quantity+&sortby=&groupon=status:Delivered,region,product_category
```

**Expected Result:**
- Filtered to status=Delivered (left-most column)
- Grouped by region (middle)
- Grouped by product_category within each region (rightmost grouped)
- Shows aggregated products, amounts, quantities (right section)

### 2. Filter by Year, Group by Quarter and Month

**Query:** Show only 2024 orders, grouped by quarter then month

**URL:**
```
?columns=year+,quarter+,month+,product_category+,amount+&sortby=&groupon=year:2024,quarter,month
```

**Expected Result:**
- year=2024 filter (leftmost)
- Q1 → Q4 groups (middle left)
- Months within quarters (middle right)
- Aggregated categories and amounts (right)

### 3. Multiple Filters + Grouping

**Query:** Show high priority USA orders, grouped by region

**URL:**
```
?columns=priority+,country+,region+,product_category+,amount+&sortby=&groupon=priority:High,country:USA,region
```

**Expected Result:**
- priority=High filter (left)
- country=USA filter (middle left)
- Grouped by region (middle right)
- Aggregated categories and amounts (right)

### 4. Hierarchical Category Analysis

**Query:** Group by product category, then by specific products

**URL:**
```
?columns=product_category+,product+,status+,amount+,quantity+&sortby=&groupon=product_category,product
```

**Expected Result:**
- Electronics
  - Headphones (2 orders)
  - Keyboard (2 orders)
  - Laptop (6 orders)
  - Monitor (2 orders)
  - Mouse (2 orders)
  - Tablet (2 orders)
  - Webcam (1 order)
- Furniture
  - Bookshelf (1 order)
  - Chair (3 orders)
  - Desk (3 orders)
  - Table (2 orders)
- Office Supplies
  - Folder (1 order)
  - Notebook (1 order)
  - Paper (1 order)
  - Pen (1 order)
  - Stapler (1 order)

### 5. Regional Sales Analysis

**Query:** Filter to delivered orders, group by country then region

**URL:**
```
?columns=status+,country+,region+,product_category+,amount+&sortby=&groupon=status:Delivered,country,region
```

**Expected Result:**
- Canada
  - North (2 orders)
  - West (1 order)
- Mexico
  - South (1 order)
- USA
  - East (4 orders)
  - North (4 orders)
  - South (3 orders)
  - West (4 orders)

### 6. Priority + Status Workflow

**Query:** Filter to high priority, group by status

**URL:**
```
?columns=priority+,status+,product+,amount+,customer+&sortby=&groupon=priority:High,status
```

**Expected Result:**
Shows all high-priority orders grouped by their current status (Delivered/Cancelled/Processing/Shipped)

### 7. Time-based Revenue Analysis

**Query:** Group by year, quarter, month to see temporal patterns

**URL:**
```
?columns=year+,quarter+,month+,amount+,quantity+&sortby=&groupon=year,quarter,month
```

**Expected Result:**
- 2024
  - Q1 (6 orders)
    - January, February, March
  - Q2 (6 orders)
    - April, May, June
  - Q3 (6 orders)
    - July, August, September
  - Q4 (6 orders)
    - October, November, December
- 2025
  - Q1 (2 orders)
    - January

## Column Order Behavior

With the new ordering system:
1. **Left:** Filtered columns (most recent filter rightmost)
2. **Middle:** Grouped columns (most recent group rightmost, creates hierarchy)
3. **Right:** Aggregated columns (data being summarized)

Example: `groupon=priority:High,country:USA,year,quarter`
- Columns: `[country, priority] [quarter, year] [month, product, amount, ...]`
- Reads as: "High priority USA orders, grouped by year then quarter"
