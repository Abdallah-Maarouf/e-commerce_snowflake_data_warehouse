# Gold Layer Data Catalog

> **Audience**: Data analysts, BI developers, data scientists, and any consumers of the Gold layer star schema.  
> **Purpose**: Understand the Gold layer tables, their relationships, and how to query them effectively.

---

## Table of Contents

- [Overview](#overview)
- [Schema Diagram](#schema-diagram)
- [How to Query the Gold Layer](#how-to-query-the-gold-layer)
- [Dimension Tables](#dimension-tables)
- [Fact Tables](#fact-tables)
- [Extension Tables](#extension-tables)
- [Common Query Patterns](#common-query-patterns)
- [Business Questions This Model Answers](#business-questions-this-model-answers)
- [Known Limitations & Caveats](#known-limitations--caveats)
- [Data Freshness & Refresh Schedule](#data-freshness--refresh-schedule)
- [Glossary](#glossary)

---

## Overview

The Gold layer is a **star schema** designed for analytics, reporting, and dashboards. It unifies data from 10 e-commerce source systems (web store, mobile app, wholesale portal, Amazon marketplace, payments, shipping, support, marketing, reviews, and mobile app events) into a single, consistent model.

### What's in the Gold Layer

| Type | Count | Tables |
|------|-------|--------|
| **Dimension Tables** | 12 | DIM_CUSTOMER, DIM_PRODUCT, DIM_DATE, DIM_CHANNEL, DIM_CATEGORY, DIM_ORDER_STATUS, DIM_PAYMENT_METHOD, DIM_PAYMENT_STATUS, DIM_LOYALTY_TIER, DIM_PAYMENT_TERMS, DIM_MARKETING_CHANNEL, DIM_CAMPAIGN |
| **Fact Tables** | 10 | FACT_ORDERS, FACT_ORDER_ITEMS, FACT_PAYMENTS, FACT_SHIPMENTS, FACT_REVIEWS, FACT_SUPPORT_TICKETS, FACT_USER_DAILY_ENGAGEMENT, FACT_CAMPAIGN_DAILY, + 3 extension tables |
| **Extension Tables** | 3 | FACT_ORDERS_MOBILE_EXT, FACT_ORDERS_WHOLESALE_EXT, FACT_ORDERS_MARKETPLACE_EXT |

### Data Volumes (as of last load)

| Table | Rows | Grain |
|-------|------|-------|
| FACT_ORDERS | 8,000 | 1 row per order |
| FACT_ORDER_ITEMS | 16,990 | 1 row per line item |
| FACT_PAYMENTS | 5,490 | 1 row per payment transaction |
| FACT_SHIPMENTS | 6,057 | 1 row per shipment |
| FACT_REVIEWS | 1,315 | 1 row per product review |
| FACT_SUPPORT_TICKETS | 960 | 1 row per support ticket |
| FACT_USER_DAILY_ENGAGEMENT | 25,152 | 1 row per user per day |
| FACT_CAMPAIGN_DAILY | 1,465 | 1 row per campaign per day |
| DIM_CUSTOMER | 1,579 | 1 row per customer version (SCD2) |
| DIM_PRODUCT | 300 | 1 row per product version (SCD2) |
| DIM_DATE | 2,098 | 1 row per calendar day |

---

## Schema Diagram

![Gold Layer Star Schema](images/silver_to_gold_lineage.png)

### Fact-Dimension Relationships

```
                        ┌─────────────────┐
                        │  DIM_CUSTOMER   │ (SCD2)
                        └────────┬────────┘
                                 │
┌──────────────┐   ┌─────────────┼─────────────┐   ┌──────────────────┐
│ DIM_CHANNEL  │───│       FACT_ORDERS          │───│ DIM_ORDER_STATUS │
└──────────────┘   │  (core — all channels)     │   └──────────────────┘
                   └─────────────┬─────────────┘
┌──────────────┐                 │                   ┌──────────────────────┐
│  DIM_DATE    │─────────────────┤                   │ FACT_ORDERS_*_EXT    │
└──────────────┘                 │                   │ (channel-specific)   │
                                 │                   └──────────────────────┘
                    ┌────────────┴────────────┐
                    │                         │
              ┌─────┴──────┐          ┌───────┴────────┐
              │FACT_PAYMENTS│          │FACT_ORDER_ITEMS │
              └─────┬──────┘          └───────┬────────┘
                    │                         │
          ┌────────┴────────┐        ┌────────┴────────┐
          │DIM_PAYMENT_METHOD│       │  DIM_PRODUCT    │ (SCD2)
          │DIM_PAYMENT_STATUS│       │  DIM_CATEGORY   │
          └─────────────────┘        └─────────────────┘
```

---

## How to Query the Gold Layer

### Connection

All Gold layer tables are in the `GOLD` schema. Queries should reference tables as:

```sql
SELECT * FROM GOLD.FACT_ORDERS LIMIT 10;
SELECT * FROM GOLD.DIM_CUSTOMER WHERE IS_CURRENT = TRUE;
```

### Basic Star Schema Join Pattern

Every fact table has `*_KEY` foreign key columns that join to dimension tables:

```sql
-- Standard pattern: Fact → Dimension via surrogate key
SELECT
    dch.CHANNEL_NAME,
    dos.STATUS_NAME,
    COUNT(*)           AS order_count,
    SUM(fo.ORDER_AMOUNT) AS total_revenue
FROM GOLD.FACT_ORDERS fo
JOIN GOLD.DIM_CHANNEL       dch ON dch.CHANNEL_KEY      = fo.CHANNEL_KEY
JOIN GOLD.DIM_ORDER_STATUS  dos ON dos.ORDER_STATUS_KEY  = fo.ORDER_STATUS_KEY
GROUP BY dch.CHANNEL_NAME, dos.STATUS_NAME
ORDER BY total_revenue DESC;
```

### SCD Type 2 — Important Rule

Three dimensions track historical changes: **DIM_CUSTOMER**, **DIM_PRODUCT**, and **DIM_CAMPAIGN**. These have multiple rows per entity (one per version).

**Always filter `IS_CURRENT = TRUE`** unless you need historical versions:

```sql
-- ✅ CORRECT: Current customer only
SELECT * FROM GOLD.DIM_CUSTOMER WHERE IS_CURRENT = TRUE;

-- ✅ CORRECT: Join to current customer in queries
SELECT dc.FULL_NAME, SUM(fo.ORDER_AMOUNT)
FROM GOLD.FACT_ORDERS fo
JOIN GOLD.DIM_CUSTOMER dc ON dc.CUSTOMER_KEY = fo.CUSTOMER_KEY
                          AND dc.IS_CURRENT = TRUE
GROUP BY dc.FULL_NAME;

-- ⚠️ WITHOUT IS_CURRENT: May return duplicate rows (one per customer version)
SELECT dc.FULL_NAME, SUM(fo.ORDER_AMOUNT)
FROM GOLD.FACT_ORDERS fo
JOIN GOLD.DIM_CUSTOMER dc ON dc.CUSTOMER_KEY = fo.CUSTOMER_KEY  -- Missing IS_CURRENT!
GROUP BY dc.FULL_NAME;
```

**Point-in-time queries** (what was the customer's loyalty tier when they placed an order):

```sql
SELECT
    fo.ORDER_ID,
    dc.LOYALTY_TIER,
    fo.ORDER_DATE
FROM GOLD.FACT_ORDERS fo
JOIN GOLD.DIM_CUSTOMER dc
    ON dc.CUSTOMER_ID = (
        SELECT dc2.CUSTOMER_ID FROM GOLD.DIM_CUSTOMER dc2
        WHERE dc2.CUSTOMER_KEY = fo.CUSTOMER_KEY
    )
    AND fo.ORDER_DATE >= dc.EFFECTIVE_DATE
    AND (fo.ORDER_DATE < dc.VALID_TO OR dc.VALID_TO IS NULL);
```

---

## Dimension Tables

### DIM_CUSTOMER

**Purpose**: Unified customer dimension across all sales channels (Web, Mobile, Wholesale).  
**SCD Type**: 2 (tracks historical changes to loyalty tier, account status, contact info)  
**Grain**: 1 row per customer per version

| Column | Type | Description |
|--------|------|-------------|
| `CUSTOMER_KEY` | INTEGER | Surrogate primary key (use for all joins) |
| `CUSTOMER_ID` | VARCHAR | Canonical customer ID (email-based from identity bridge) |
| `EMAIL_ADDRESS` | VARCHAR | Email address (primary cross-channel join key) |
| `FIRST_NAME` | VARCHAR | First name |
| `LAST_NAME` | VARCHAR | Last name |
| `FULL_NAME` | VARCHAR | Concatenated first + last name |
| `PHONE_NUMBER` | VARCHAR | Phone number (Web customers only — NULL for Mobile/Wholesale) |
| `CUSTOMER_TYPE` | VARCHAR | `B2C` or `B2B` (B2B if exists in Wholesale) |
| `COMPANY_NAME` | VARCHAR | Company name (B2B customers only — NULL for B2C) |
| `COUNTRY_CODE` | VARCHAR | ISO 3166-1 alpha-2 country code |
| `CITY_NAME` | VARCHAR | City (Web customers only) |
| `POSTAL_CODE` | VARCHAR | Postal code (Web customers only) |
| `CURRENCY_CODE` | VARCHAR | Preferred currency code |
| `REGISTRATION_DATE` | DATE | Web store registration date (NULL for non-Web) |
| `LOYALTY_TIER` | VARCHAR | Canonical tier: standard / silver / gold / platinum |
| `LOYALTY_TIER_RANK` | INTEGER | Numeric tier ranking (1=standard, 4=platinum) |
| `ACCOUNT_STATUS` | VARCHAR | active / suspended / deleted |
| `MARKETING_OPT_IN` | BOOLEAN | Marketing consent flag |
| `PRIMARY_CHANNEL` | VARCHAR | Customer's primary sales channel (web / mobile / wholesale) |
| `CHANNELS_USED` | VARCHAR | Comma-separated channels where customer has ordered |
| `EFFECTIVE_DATE` | DATE | SCD2: row valid from this date |
| `VALID_TO` | DATE | SCD2: row valid until this date (NULL = current row) |
| `IS_CURRENT` | BOOLEAN | SCD2: TRUE for the latest version of each customer |

**Important Notes**:
- Marketplace customers are **NOT** in this table — Amazon masks buyer email, so identity resolution is impossible
- Field completeness varies by source channel — Web customers have the most complete profiles; Wholesale customers have only email and company name
- Always use `IS_CURRENT = TRUE` when joining to fact tables, unless doing point-in-time analysis

---

### DIM_PRODUCT

**Purpose**: Product master dimension derived from all order item tables.  
**SCD Type**: 2 (tracks changes to product status and primary category)  
**Grain**: 1 row per product SKU per version

| Column | Type | Description |
|--------|------|-------------|
| `PRODUCT_KEY` | INTEGER | Surrogate primary key |
| `PRODUCT_SKU` | VARCHAR | Canonical SKU format: `SKU-XXXXX` |
| `PRODUCT_NAME` | VARCHAR | Product name |
| `PRIMARY_CATEGORY_CODE` | VARCHAR | Primary category code (e.g., ELEC, CLTH, HOME) |
| `PRIMARY_CATEGORY_NAME` | VARCHAR | Primary category display name |
| `ALL_CATEGORIES` | VARCHAR | Comma-separated list of all categories |
| `CURRENT_UNIT_PRICE` | DECIMAL | Latest unit price (Type 1 overwrite) |
| `AVERAGE_UNIT_PRICE` | DECIMAL | Average unit price across all orders |
| `MIN_UNIT_PRICE` | DECIMAL | Lowest recorded unit price |
| `MAX_UNIT_PRICE` | DECIMAL | Highest recorded unit price |
| `PRODUCT_STATUS` | VARCHAR | Active or Discontinued (Discontinued if no sales in 180 days) |
| `FIRST_SALE_DATE` | DATE | Date of first sale |
| `LAST_SALE_DATE` | DATE | Date of most recent sale |
| `EFFECTIVE_DATE` | DATE | SCD2: row valid from this date |
| `VALID_TO` | DATE | SCD2: row valid until (NULL = current) |
| `IS_CURRENT` | BOOLEAN | SCD2: TRUE for latest version |

**Important Notes**:
- Price columns are Type 1 (overwrite each load) — detailed pricing history is in FACT_ORDER_ITEMS
- Total revenue and quantity sold are **NOT** stored here — always derive from FACT_ORDER_ITEMS to avoid stale pre-aggregated values
- Always use `IS_CURRENT = TRUE` for current product attributes

---

### DIM_DATE

**Purpose**: Calendar dimension for time-based analysis.  
**Grain**: 1 row per calendar day

| Column | Type | Description |
|--------|------|-------------|
| `DATE_KEY` | INTEGER | Primary key in `YYYYMMDD` format (e.g., 20240115) |
| `FULL_DATE` | DATE | Full date value |
| `DAY_OF_WEEK` | INTEGER | 1 (Monday) through 7 (Sunday) |
| `DAY_NAME` | VARCHAR | Monday, Tuesday, ..., Sunday |
| `DAY_OF_MONTH` | INTEGER | 1–31 |
| `DAY_OF_YEAR` | INTEGER | 1–366 |
| `WEEK_OF_YEAR` | INTEGER | ISO week number |
| `MONTH` | INTEGER | 1–12 |
| `MONTH_NAME` | VARCHAR | January, February, ..., December |
| `QUARTER` | INTEGER | 1–4 |
| `YEAR` | INTEGER | 4-digit year |
| `IS_WEEKEND` | BOOLEAN | TRUE for Saturday/Sunday |
| `IS_HOLIDAY` | BOOLEAN | TRUE if a public holiday |
| `HOLIDAY_NAME` | VARCHAR | Name of the holiday (NULL if not a holiday) |
| `FISCAL_YEAR` | INTEGER | Fiscal year |
| `FISCAL_QUARTER` | INTEGER | Fiscal quarter |
| `FISCAL_MONTH` | INTEGER | Fiscal month |

**Usage**: Join fact table date keys to `DIM_DATE.DATE_KEY`:
```sql
SELECT dd.MONTH_NAME, dd.YEAR, SUM(fo.ORDER_AMOUNT)
FROM GOLD.FACT_ORDERS fo
JOIN GOLD.DIM_DATE dd ON dd.DATE_KEY = fo.ORDER_DATE_KEY
GROUP BY dd.YEAR, dd.MONTH_NAME
ORDER BY dd.YEAR, MIN(dd.MONTH);
```

---

### DIM_CHANNEL

**Purpose**: Sales channel dimension (where the customer purchases).  
**Grain**: 1 row per channel (4 rows total)

| Column | Type | Description |
|--------|------|-------------|
| `CHANNEL_KEY` | INTEGER | Surrogate primary key |
| `CHANNEL_ID` | VARCHAR | Business key: `web`, `mobile`, `wholesale`, `marketplace` |
| `CHANNEL_NAME` | VARCHAR | Display name: Web Store, Mobile App, Wholesale Portal, Marketplace |
| `CHANNEL_TYPE` | VARCHAR | `B2C` (web, mobile, marketplace) or `B2B` (wholesale) |
| `IS_ACTIVE` | BOOLEAN | Always TRUE |

**Important**: This is the **sales** channel (where orders come from). For **marketing** channels (how customers are acquired), see DIM_MARKETING_CHANNEL.

---

### DIM_CATEGORY

**Purpose**: Product category dimension (10 canonical categories).  
**Grain**: 1 row per category

| Column | Type | Description |
|--------|------|-------------|
| `CATEGORY_KEY` | INTEGER | Surrogate primary key |
| `CATEGORY_CODE` | VARCHAR | Canonical code: ELEC, CLTH, HOME, SPRT, BOOK, BEAU, TOYS, FOOD, TOOL, PET |
| `CATEGORY_NAME` | VARCHAR | Display name: Electronics, Clothing & Apparel, Home & Kitchen, etc. |
| `IS_ACTIVE` | BOOLEAN | Always TRUE |

---

### DIM_ORDER_STATUS

**Purpose**: Canonical order status dimension.  
**Grain**: 1 row per status (5 rows)

| Column | Type | Description |
|--------|------|-------------|
| `ORDER_STATUS_KEY` | INTEGER | Surrogate primary key |
| `STATUS_CODE` | VARCHAR | Canonical status: completed, processing, shipped, cancelled, returned |
| `STATUS_NAME` | VARCHAR | Display name |
| `STATUS_DESCRIPTION` | VARCHAR | Detailed description |
| `STATUS_CATEGORY` | VARCHAR | Open (processing, shipped), Closed (completed), Cancelled (cancelled, returned) |
| `IS_ACTIVE` | BOOLEAN | Always TRUE |

**Note**: All source system status codes have been mapped to these 5 canonical values. You never need to decode raw status codes.

---

### DIM_PAYMENT_METHOD

**Purpose**: Payment method dimension.  
**Grain**: 1 row per payment method (7 rows)

| Column | Type | Description |
|--------|------|-------------|
| `PAYMENT_METHOD_KEY` | INTEGER | Surrogate primary key |
| `PAYMENT_METHOD_CODE` | VARCHAR | Code: CC, DC, PP, APY, GPY, BT, BNPL |
| `PAYMENT_METHOD_NAME` | VARCHAR | Credit Card, Debit Card, PayPal, Apple Pay, Google Pay, Bank Transfer, Buy Now Pay Later |
| `CATEGORY` | VARCHAR | card, digital_wallet, bank_transfer, bnpl |
| `IS_ACTIVE` | BOOLEAN | Always TRUE |

---

### DIM_PAYMENT_STATUS

**Purpose**: Payment status dimension.  
**Grain**: 1 row per status (4 rows)

| Column | Type | Description |
|--------|------|-------------|
| `PAYMENT_STATUS_KEY` | INTEGER | Surrogate primary key |
| `STATUS_CODE` | VARCHAR | succeeded, failed, pending, refunded |
| `STATUS_NAME` | VARCHAR | Display name |
| `IS_SUCCESSFUL` | BOOLEAN | TRUE only for `succeeded` |
| `IS_ACTIVE` | BOOLEAN | Always TRUE |

---

### DIM_LOYALTY_TIER

**Purpose**: Customer loyalty tier dimension.  
**Grain**: 1 row per tier (4 rows)

| Column | Type | Description |
|--------|------|-------------|
| `LOYALTY_TIER_KEY` | INTEGER | Surrogate primary key |
| `TIER_CODE` | VARCHAR | standard, silver, gold, platinum |
| `TIER_NAME` | VARCHAR | Display name |
| `TIER_RANK` | INTEGER | Numeric rank: 1 (standard) → 4 (platinum) |
| `IS_ACTIVE` | BOOLEAN | Always TRUE |

---

### DIM_PAYMENT_TERMS

**Purpose**: B2B payment terms dimension (wholesale channel only).  
**Grain**: 1 row per payment term (5 rows)

| Column | Type | Description |
|--------|------|-------------|
| `PAYMENT_TERMS_KEY` | INTEGER | Surrogate primary key |
| `PAYMENT_TERMS_CODE` | VARCHAR | NET30, NET60, NET90, COD, PREPAID |
| `PAYMENT_TERMS_NAME` | VARCHAR | Display name |
| `DAYS_TO_PAY` | INTEGER | Days until payment due (30, 60, 90, 0, 0) |
| `IS_ACTIVE` | BOOLEAN | Always TRUE |

---

### DIM_MARKETING_CHANNEL

**Purpose**: Marketing delivery channel dimension — how customers are acquired/reached.  
**Grain**: 1 row per marketing channel (11 rows)

| Column | Type | Description |
|--------|------|-------------|
| `MARKETING_CHANNEL_KEY` | INTEGER | Surrogate primary key |
| `MARKETING_CHANNEL_ID` | VARCHAR | Channel identifier (email, social_media, search, display, referral, etc.) |
| `MARKETING_CHANNEL_NAME` | VARCHAR | Display name |
| `IS_PAID` | BOOLEAN | TRUE for paid channels (search ads, display ads, paid social) |
| `IS_ACTIVE` | BOOLEAN | Always TRUE |

**Important**: This is **NOT** the same as DIM_CHANNEL. DIM_CHANNEL = where the customer buys (web/mobile/wholesale/marketplace). DIM_MARKETING_CHANNEL = how the customer was reached (email/social/search/display).

---

### DIM_CAMPAIGN

**Purpose**: Marketing campaign dimension.  
**SCD Type**: 2 (tracks changes to campaign name, status, end date)  
**Grain**: 1 row per campaign per version

| Column | Type | Description |
|--------|------|-------------|
| `CAMPAIGN_KEY` | INTEGER | Surrogate primary key |
| `CAMPAIGN_ID` | VARCHAR | Business key (e.g., CMP0001) |
| `CAMPAIGN_NAME` | VARCHAR | Campaign name |
| `MARKETING_CHANNEL_KEY` | INTEGER | FK → DIM_MARKETING_CHANNEL |
| `TARGET_SEGMENT` | VARCHAR | Target audience segment (New Users, Returning, High Value, etc.) |
| `START_DATE` | DATE | Campaign start date |
| `END_DATE` | DATE | Campaign end date |
| `CAMPAIGN_STATUS` | VARCHAR | active or ended |
| `EFFECTIVE_DATE` | DATE | SCD2: row valid from |
| `VALID_TO` | DATE | SCD2: row valid until (NULL = current) |
| `IS_CURRENT` | BOOLEAN | SCD2: TRUE for latest version |

---

## Fact Tables

### FACT_ORDERS

**Purpose**: Core order-level facts — unified across all 4 sales channels.  
**Grain**: 1 row per order (8,000 rows)

| Column | Type | Description | Joins To |
|--------|------|-------------|----------|
| `ORDER_KEY` | INTEGER | Surrogate PK — use this for joining to other fact tables | — |
| `ORDER_ID` | INTEGER | Business order ID (canonical across all sources) | Degenerate dimension |
| `CUSTOMER_KEY` | INTEGER | FK to customer dimension. **NULL for marketplace orders** (Amazon masks buyer email) | DIM_CUSTOMER |
| `ORDER_DATE_KEY` | INTEGER | FK to date dimension (YYYYMMDD) | DIM_DATE |
| `CHANNEL_KEY` | INTEGER | FK to channel dimension | DIM_CHANNEL |
| `ORDER_STATUS_KEY` | INTEGER | FK to order status dimension | DIM_ORDER_STATUS |
| `ORDER_DATE` | TIMESTAMP | Precise order timestamp (UTC) | — |
| `ORDER_AMOUNT` | DECIMAL(14,2) | Total order amount in original currency | Measure |
| `ORDER_AMOUNT_USD` | DECIMAL(14,2) | Total order amount in USD | Measure |
| `EXCHANGE_RATE` | DECIMAL(10,6) | USD exchange rate at order date (1.0 for USD orders) | Measure |
| `TAX_AMOUNT` | DECIMAL(14,2) | Tax amount (**wholesale orders only** — NULL for other channels) | Measure |
| `CURRENCY_CODE` | VARCHAR | ISO 4217 currency code | Degenerate dimension |
| `PROMO_CODE` | VARCHAR | Promotional code used (NULL if none) | Degenerate dimension |
| `_SILVER_LOAD_TIMESTAMP` | TIMESTAMP | Watermark for incremental loading | Metadata |

**Design Note**: Payment method is NOT stored here because an order can have multiple payment attempts. To get an order's payment method, join to `FACT_PAYMENTS` and filter to `IS_FIRST_TRANSACTION = TRUE` or use `DIM_PAYMENT_STATUS.IS_SUCCESSFUL = TRUE`.

**Channel Distribution**:
| Channel | Orders | % of Total |
|---------|--------|-----------|
| Web Store | 3,600 | 45% |
| Mobile App | 2,400 | 30% |
| Wholesale | 1,200 | 15% |
| Marketplace | 800 | 10% |

---

### FACT_ORDER_ITEMS

**Purpose**: Line item-level facts for detailed product analysis.  
**Grain**: 1 row per order line item (16,990 rows)

| Column | Type | Description | Joins To |
|--------|------|-------------|----------|
| `ORDER_ITEM_KEY` | INTEGER | Surrogate PK | — |
| `ORDER_ID` | INTEGER | Business order ID | Degenerate dimension |
| `ORDER_KEY` | INTEGER | FK to parent order | FACT_ORDERS |
| `PRODUCT_KEY` | INTEGER | FK to product dimension | DIM_PRODUCT |
| `CUSTOMER_KEY` | INTEGER | FK to customer dimension (inherited from order) | DIM_CUSTOMER |
| `ORDER_DATE_KEY` | INTEGER | FK to date dimension (inherited from order) | DIM_DATE |
| `CHANNEL_KEY` | INTEGER | FK to channel dimension | DIM_CHANNEL |
| `CATEGORY_KEY` | INTEGER | FK to category dimension | DIM_CATEGORY |
| `QUANTITY` | INTEGER | Quantity ordered | Measure |
| `UNIT_PRICE` | DECIMAL(10,2) | Unit price | Measure |
| `LINE_TOTAL` | DECIMAL(10,2) | Line total (quantity × unit price, after discounts) | Measure |
| `IS_RETURNED` | BOOLEAN | TRUE if this item was returned (**Web orders only** — NULL for other channels) | Flag |
| `MARKETPLACE_FEE` | DECIMAL(10,2) | Amazon marketplace referral fee (**Marketplace orders only** — NULL for other channels) | Measure |

**Usage**: This is your go-to table for product-level analysis. Always derive revenue and quantity metrics from here (not from DIM_PRODUCT).

---

### FACT_PAYMENTS

**Purpose**: Payment transaction facts — covers Web and Mobile channels only.  
**Grain**: 1 row per payment transaction (5,490 rows)

| Column | Type | Description | Joins To |
|--------|------|-------------|----------|
| `PAYMENT_KEY` | INTEGER | Surrogate PK | — |
| `TRANSACTION_ID` | VARCHAR | Gateway transaction ID | Degenerate dimension |
| `ORDER_ID` | INTEGER | Order ID | Degenerate dimension |
| `ORDER_KEY` | INTEGER | FK to order | FACT_ORDERS |
| `PAYMENT_METHOD_KEY` | INTEGER | FK to payment method | DIM_PAYMENT_METHOD |
| `PAYMENT_STATUS_KEY` | INTEGER | FK to payment status | DIM_PAYMENT_STATUS |
| `TRANSACTION_DATE_KEY` | INTEGER | FK to date | DIM_DATE |
| `CHANNEL_KEY` | INTEGER | FK to channel | DIM_CHANNEL |
| `PROCESSED_DATETIME` | TIMESTAMP | Precise processing timestamp | — |
| `GROSS_AMOUNT` | DECIMAL(10,2) | Gross payment amount | Measure |
| `FEE_AMOUNT` | DECIMAL(10,2) | Processing fee | Measure |
| `NET_AMOUNT` | DECIMAL(10,2) | Net amount after fees | Measure |
| `CURRENCY_CODE` | VARCHAR | Payment currency | Degenerate dimension |
| `FAILURE_REASON_CODE` | VARCHAR | Failure reason (NULL if successful): F01=insufficient_funds, F02=card_expired, F03=fraud_suspected, F04=gateway_timeout, F05=invalid_cvv | Degenerate dimension |
| `IS_FIRST_TRANSACTION` | BOOLEAN | TRUE if this is the customer's first-ever payment | Flag |
| `GATEWAY_REGION` | VARCHAR | Processing region: US, EU, APAC | Degenerate dimension |

**Important Notes**:
- Covers **Web and Mobile orders only** — Wholesale uses invoice terms (NET30/NET60), Marketplace uses Amazon Pay. Neither appears in this table.
- An order can have **multiple payment rows** (retries after failures). Filter to `DIM_PAYMENT_STATUS.IS_SUCCESSFUL = TRUE` for successful payments only.

---

### FACT_SHIPMENTS

**Purpose**: Shipment and delivery tracking facts.  
**Grain**: 1 row per shipment (6,057 rows)

| Column | Type | Description | Joins To |
|--------|------|-------------|----------|
| `SHIPMENT_KEY` | INTEGER | Surrogate PK | — |
| `SHIPMENT_ID` | VARCHAR | Shipment tracking ID | Degenerate dimension |
| `ORDER_ID` | INTEGER | Order ID | Degenerate dimension |
| `ORDER_KEY` | INTEGER | FK to order | FACT_ORDERS |
| `SHIPMENT_DATE_KEY` | INTEGER | FK to pickup date | DIM_DATE |
| `DELIVERY_DATE_KEY` | INTEGER | FK to delivery date (NULL for in-transit) | DIM_DATE |
| `CHANNEL_KEY` | INTEGER | FK to channel | DIM_CHANNEL |
| `ORIGIN_FACILITY` | VARCHAR | Warehouse code: WH-NJ-01, WH-CA-01, WH-TX-01, WH-LHR-01, WH-MAN-01, WH-FRA-01, WH-AMS-01 | Degenerate dimension |
| `PICKUP_DATETIME` | TIMESTAMP | Shipment pickup timestamp | — |
| `DELIVERY_DATETIME` | TIMESTAMP | Delivery timestamp (NULL for in-transit/undelivered) | — |
| `PACKAGE_WEIGHT_KG` | DECIMAL(10,3) | Package weight normalized to kilograms | Measure |
| `WEIGHT_UNIT_ORIGINAL` | VARCHAR | Original weight unit (GRM or KGM) | Degenerate dimension |
| `SHIPMENT_STATUS` | VARCHAR | LABEL_CREATED, PICKED_UP, IN_TRANSIT, OUT_FOR_DELIVERY, DELIVERED, DELIVERY_FAILED, RETURNED | Degenerate dimension |
| `SIGNATURE_REQUIRED` | BOOLEAN | Signature required for delivery | Flag |
| `INSURANCE_VALUE` | DECIMAL(10,2) | Insurance value (0.00 for orders under $300) | Measure |
| `DAYS_TO_DELIVER` | INTEGER | Days from pickup to delivery (NULL for in-transit) | Derived measure |
| `ON_TIME_DELIVERY_FLAG` | BOOLEAN | TRUE if delivered within 7 days | Derived flag |

---

### FACT_REVIEWS

**Purpose**: Product review facts.  
**Grain**: 1 row per product review (1,315 rows)

| Column | Type | Description | Joins To |
|--------|------|-------------|----------|
| `REVIEW_KEY` | INTEGER | Surrogate PK | — |
| `REVIEW_ID` | INTEGER | Business review ID | Degenerate dimension |
| `PRODUCT_KEY` | INTEGER | FK to product | DIM_PRODUCT |
| `CATEGORY_KEY` | INTEGER | FK to category | DIM_CATEGORY |
| `REVIEW_DATE_KEY` | INTEGER | FK to date | DIM_DATE |
| `REVIEWER_HANDLE` | VARCHAR | Anonymous reviewer username | Degenerate dimension |
| `STAR_RATING` | INTEGER | Star rating 1–5 | Measure |
| `VERIFIED_PURCHASE` | BOOLEAN | TRUE if reviewer made a verified purchase | Flag |
| `VERIFIED_SOURCE` | VARCHAR | Source channel of verified purchase (web_store, mobile_app, etc.) | Degenerate dimension |
| `MODERATION_STATUS` | VARCHAR | approved or flagged | Degenerate dimension |
| `HELPFUL_VOTES` | INTEGER | Number of helpful votes | Measure |

**Important**: Reviews **cannot** be linked to customers or orders. The reviewer is identified only by an anonymous handle. This is a data source limitation.

---

### FACT_SUPPORT_TICKETS

**Purpose**: Customer support ticket facts.  
**Grain**: 1 row per support ticket (960 rows)

| Column | Type | Description | Joins To |
|--------|------|-------------|----------|
| `TICKET_KEY` | INTEGER | Surrogate PK | — |
| `TICKET_ID` | INTEGER | Business ticket ID | Degenerate dimension |
| `ORDER_ID` | INTEGER | Related order ID | Degenerate dimension |
| `ORDER_KEY` | INTEGER | FK to order | FACT_ORDERS |
| `CUSTOMER_KEY` | INTEGER | FK to customer (resolved via requester email) | DIM_CUSTOMER |
| `CREATED_DATE_KEY` | INTEGER | FK to ticket creation date | DIM_DATE |
| `SOLVED_DATE_KEY` | INTEGER | FK to solve date (NULL if open) | DIM_DATE |
| `CHANNEL_KEY` | INTEGER | FK to order source channel | DIM_CHANNEL |
| `ASSIGNEE` | VARCHAR | Support agent ID | Degenerate dimension |
| `TAGS` | VARCHAR | Comma-separated tags | Degenerate dimension |
| `TICKET_STATUS` | VARCHAR | open, pending, solved | Degenerate dimension |
| `CREATED_DATETIME` | TIMESTAMP | Ticket creation timestamp | — |
| `SOLVED_DATETIME` | TIMESTAMP | Solve timestamp (NULL if open) | — |
| `FIRST_REPLY_HOURS` | DECIMAL(5,2) | Hours to first agent reply | Measure |
| `CSAT_SCORE` | DECIMAL(5,2) | Customer satisfaction (5=good, 3=neutral, 1=bad) | Measure |
| `DAYS_TO_SOLVE` | INTEGER | Days from creation to solution (NULL if open) | Derived measure |
| `IS_SOLVED` | BOOLEAN | TRUE if ticket status is solved | Derived flag |

---

### FACT_USER_DAILY_ENGAGEMENT

**Purpose**: Daily user engagement metrics aggregated from mobile app events.  
**Grain**: 1 row per user per day (25,152 rows)

| Column | Type | Description | Joins To |
|--------|------|-------------|----------|
| `USER_DAILY_KEY` | INTEGER | Surrogate PK | — |
| `USER_ID` | VARCHAR | Mobile app user ID (USR_XXXXXX) | Degenerate dimension |
| `CUSTOMER_KEY` | INTEGER | FK to customer (NULL for guest sessions) | DIM_CUSTOMER |
| `ACTIVITY_DATE_KEY` | INTEGER | FK to date | DIM_DATE |
| `ACTIVITY_DATE` | DATE | Activity date | — |
| `APP_VERSION` | VARCHAR | App version used that day (latest) | Degenerate dimension |
| `IS_AUTHENTICATED` | BOOLEAN | TRUE if any authenticated event that day | Flag |
| `IS_VPN_DETECTED` | BOOLEAN | TRUE if VPN detected in any event | Flag |
| `SESSION_COUNT` | INTEGER | Number of distinct sessions | Measure |
| `TOTAL_EVENTS` | INTEGER | Total events across all sessions | Measure |
| `FIRST_EVENT_TIME` | TIMESTAMP | First event of the day | — |
| `LAST_EVENT_TIME` | TIMESTAMP | Last event of the day | — |
| `ACTIVE_MINUTES` | INTEGER | Minutes from first to last event | Measure |
| `LOGIN_COUNT` | INTEGER | Account login events | Measure |
| `PRODUCT_VIEW_COUNT` | INTEGER | Product page view events | Measure |
| `CATEGORY_BROWSE_COUNT` | INTEGER | Category browsing events | Measure |
| `SEARCH_COUNT` | INTEGER | Search events | Measure |
| `ADD_TO_CART_COUNT` | INTEGER | Add-to-cart events | Measure |
| `CHECKOUT_START_COUNT` | INTEGER | Checkout started events | Measure |
| `CHECKOUT_COMPLETE_COUNT` | INTEGER | Checkout completed events | Measure |
| `WISHLIST_ADD_COUNT` | INTEGER | Wishlist add events | Measure |
| `UNIQUE_PRODUCTS_VIEWED` | INTEGER | Distinct products viewed | Measure |
| `UNIQUE_CATEGORIES_BROWSED` | INTEGER | Distinct categories browsed | Measure |

**Funnel Analysis**: Use the event counts to build conversion funnels:
```
PRODUCT_VIEW → ADD_TO_CART → CHECKOUT_START → CHECKOUT_COMPLETE
```

---

### FACT_CAMPAIGN_DAILY

**Purpose**: Marketing campaign daily performance metrics.  
**Grain**: 1 row per campaign per day (1,465 rows)

| Column | Type | Description | Joins To |
|--------|------|-------------|----------|
| `CAMPAIGN_DAILY_KEY` | INTEGER | Surrogate PK | — |
| `CAMPAIGN_KEY` | INTEGER | FK to campaign | DIM_CAMPAIGN |
| `DATE_KEY` | INTEGER | FK to date | DIM_DATE |
| `MARKETING_CHANNEL_KEY` | INTEGER | FK to marketing channel | DIM_MARKETING_CHANNEL |
| `DATE` | DATE | Activity date | — |
| `AMOUNT_SPENT` | DECIMAL(10,2) | Daily spend | Measure |
| `IMPRESSIONS` | INTEGER | Number of impressions | Measure |
| `CLICKS` | INTEGER | Number of clicks | Measure |
| `CONVERSIONS` | INTEGER | Number of conversions | Measure |
| `CTR_PERCENT` | DECIMAL(8,4) | Click-through rate (%) | Measure |
| `CPC_AMOUNT` | DECIMAL(10,4) | Cost per click (NULL when clicks = 0) | Measure |
| `CPA_AMOUNT` | DECIMAL(10,4) | Cost per acquisition (NULL when conversions = 0) | Measure |

---

## Extension Tables

Extension tables contain **channel-specific attributes** that don't apply to all orders. They share `ORDER_KEY` with FACT_ORDERS and are joined via LEFT JOIN.

### FACT_ORDERS_MOBILE_EXT

**Purpose**: Mobile-specific order attributes.  
**Grain**: 1 row per mobile order (2,400 rows)

| Column | Type | Description |
|--------|------|-------------|
| `ORDER_KEY` | INTEGER | PK + FK → FACT_ORDERS |
| `PLATFORM_TYPE` | VARCHAR | `iOS` or `Android` |
| `SOURCE_ORDER_ID` | VARCHAR | Mobile platform order ID (e.g., MOB-00008821) |

### FACT_ORDERS_WHOLESALE_EXT

**Purpose**: Wholesale (B2B) specific order attributes.  
**Grain**: 1 row per wholesale order (1,200 rows)

| Column | Type | Description |
|--------|------|-------------|
| `ORDER_KEY` | INTEGER | PK + FK → FACT_ORDERS |
| `BUYER_PO_NUMBER` | VARCHAR | Buyer's purchase order reference (e.g., PO-482931) |
| `PAYMENT_TERMS_KEY` | INTEGER | FK → DIM_PAYMENT_TERMS (NET30, NET60, etc.) |
| `ORDER_AMOUNT_EXCL_TAX` | DECIMAL(14,2) | Order total excluding tax |

### FACT_ORDERS_MARKETPLACE_EXT

**Purpose**: Amazon Marketplace specific order attributes.  
**Grain**: 1 row per marketplace order (800 rows)

| Column | Type | Description |
|--------|------|-------------|
| `ORDER_KEY` | INTEGER | PK + FK → FACT_ORDERS |
| `AMAZON_ORDER_ID` | VARCHAR | Amazon's order ID (e.g., 143-4829103-8291023) |
| `FULFILLMENT_CHANNEL` | VARCHAR | `FBA` (Fulfilled by Amazon) or `FBM` (Fulfilled by Merchant) |

### How to Use Extension Tables

```sql
-- Get iOS vs Android order breakdown
SELECT
    mext.PLATFORM_TYPE,
    COUNT(*)              AS orders,
    SUM(fo.ORDER_AMOUNT)  AS revenue
FROM GOLD.FACT_ORDERS fo
JOIN GOLD.FACT_ORDERS_MOBILE_EXT mext ON mext.ORDER_KEY = fo.ORDER_KEY
GROUP BY mext.PLATFORM_TYPE;

-- Get wholesale orders with payment terms
SELECT
    dpt.PAYMENT_TERMS_NAME,
    dpt.DAYS_TO_PAY,
    COUNT(*)              AS orders,
    SUM(fo.ORDER_AMOUNT)  AS revenue
FROM GOLD.FACT_ORDERS fo
JOIN GOLD.FACT_ORDERS_WHOLESALE_EXT wext ON wext.ORDER_KEY = fo.ORDER_KEY
JOIN GOLD.DIM_PAYMENT_TERMS dpt ON dpt.PAYMENT_TERMS_KEY = wext.PAYMENT_TERMS_KEY
GROUP BY dpt.PAYMENT_TERMS_NAME, dpt.DAYS_TO_PAY;

-- Get FBA vs FBM breakdown for marketplace
SELECT
    mkext.FULFILLMENT_CHANNEL,
    COUNT(*) AS orders
FROM GOLD.FACT_ORDERS fo
JOIN GOLD.FACT_ORDERS_MARKETPLACE_EXT mkext ON mkext.ORDER_KEY = fo.ORDER_KEY
GROUP BY mkext.FULFILLMENT_CHANNEL;
```

---

## Common Query Patterns

### 1. Revenue by Channel

```sql
SELECT
    dch.CHANNEL_NAME,
    COUNT(*)              AS order_count,
    SUM(fo.ORDER_AMOUNT)  AS total_revenue,
    AVG(fo.ORDER_AMOUNT)  AS avg_order_value
FROM GOLD.FACT_ORDERS fo
JOIN GOLD.DIM_CHANNEL dch ON dch.CHANNEL_KEY = fo.CHANNEL_KEY
GROUP BY dch.CHANNEL_NAME
ORDER BY total_revenue DESC;
```

### 2. Monthly Revenue Trend

```sql
SELECT
    dd.YEAR,
    dd.MONTH_NAME,
    dd.MONTH,
    dch.CHANNEL_NAME,
    SUM(fo.ORDER_AMOUNT) AS revenue
FROM GOLD.FACT_ORDERS fo
JOIN GOLD.DIM_DATE dd ON dd.DATE_KEY = fo.ORDER_DATE_KEY
JOIN GOLD.DIM_CHANNEL dch ON dch.CHANNEL_KEY = fo.CHANNEL_KEY
GROUP BY dd.YEAR, dd.MONTH_NAME, dd.MONTH, dch.CHANNEL_NAME
ORDER BY dd.YEAR, dd.MONTH;
```

### 3. Top 10 Products by Revenue

```sql
SELECT
    dp.PRODUCT_SKU,
    dp.PRODUCT_NAME,
    dc.CATEGORY_NAME,
    SUM(foi.QUANTITY)    AS units_sold,
    SUM(foi.LINE_TOTAL)  AS total_revenue
FROM GOLD.FACT_ORDER_ITEMS foi
JOIN GOLD.DIM_PRODUCT dp ON dp.PRODUCT_KEY = foi.PRODUCT_KEY AND dp.IS_CURRENT = TRUE
JOIN GOLD.DIM_CATEGORY dc ON dc.CATEGORY_KEY = foi.CATEGORY_KEY
GROUP BY dp.PRODUCT_SKU, dp.PRODUCT_NAME, dc.CATEGORY_NAME
ORDER BY total_revenue DESC
LIMIT 10;
```

### 4. Customer Lifetime Value (Top 20)

```sql
SELECT
    dc.CUSTOMER_ID,
    dc.FULL_NAME,
    dc.EMAIL_ADDRESS,
    dc.LOYALTY_TIER,
    dc.CHANNELS_USED,
    COUNT(DISTINCT fo.ORDER_ID) AS order_count,
    SUM(fo.ORDER_AMOUNT)        AS lifetime_value
FROM GOLD.FACT_ORDERS fo
JOIN GOLD.DIM_CUSTOMER dc ON dc.CUSTOMER_KEY = fo.CUSTOMER_KEY AND dc.IS_CURRENT = TRUE
GROUP BY dc.CUSTOMER_ID, dc.FULL_NAME, dc.EMAIL_ADDRESS, dc.LOYALTY_TIER, dc.CHANNELS_USED
ORDER BY lifetime_value DESC
LIMIT 20;
```

### 5. Order Fulfillment Pipeline

```sql
SELECT
    dos.STATUS_NAME           AS order_status,
    dos.STATUS_CATEGORY,
    dch.CHANNEL_NAME,
    COUNT(*)                  AS order_count,
    SUM(fo.ORDER_AMOUNT)      AS revenue
FROM GOLD.FACT_ORDERS fo
JOIN GOLD.DIM_ORDER_STATUS dos ON dos.ORDER_STATUS_KEY = fo.ORDER_STATUS_KEY
JOIN GOLD.DIM_CHANNEL dch ON dch.CHANNEL_KEY = fo.CHANNEL_KEY
GROUP BY dos.STATUS_NAME, dos.STATUS_CATEGORY, dch.CHANNEL_NAME
ORDER BY dch.CHANNEL_NAME, dos.STATUS_CATEGORY;
```

### 6. Payment Success Rate by Method

```sql
SELECT
    dpm.PAYMENT_METHOD_NAME,
    dps.STATUS_NAME,
    COUNT(*)              AS transactions,
    SUM(fp.GROSS_AMOUNT)  AS total_amount
FROM GOLD.FACT_PAYMENTS fp
JOIN GOLD.DIM_PAYMENT_METHOD dpm ON dpm.PAYMENT_METHOD_KEY = fp.PAYMENT_METHOD_KEY
JOIN GOLD.DIM_PAYMENT_STATUS dps ON dps.PAYMENT_STATUS_KEY = fp.PAYMENT_STATUS_KEY
GROUP BY dpm.PAYMENT_METHOD_NAME, dps.STATUS_NAME
ORDER BY dpm.PAYMENT_METHOD_NAME, dps.STATUS_NAME;
```

### 7. Shipping Performance by Warehouse

```sql
SELECT
    fs.ORIGIN_FACILITY,
    COUNT(*)                          AS shipments,
    AVG(fs.DAYS_TO_DELIVER)           AS avg_days_to_deliver,
    SUM(IFF(fs.ON_TIME_DELIVERY_FLAG, 1, 0)) * 100.0 / COUNT(*) AS on_time_pct
FROM GOLD.FACT_SHIPMENTS fs
WHERE fs.DELIVERY_DATETIME IS NOT NULL  -- exclude in-transit
GROUP BY fs.ORIGIN_FACILITY
ORDER BY on_time_pct DESC;
```

### 8. Support Ticket Resolution Metrics

```sql
SELECT
    dch.CHANNEL_NAME,
    COUNT(*)                              AS total_tickets,
    SUM(IFF(fst.IS_SOLVED, 1, 0))        AS solved,
    AVG(fst.FIRST_REPLY_HOURS)            AS avg_first_reply_hrs,
    AVG(fst.DAYS_TO_SOLVE)                AS avg_days_to_solve,
    AVG(fst.CSAT_SCORE)                   AS avg_csat
FROM GOLD.FACT_SUPPORT_TICKETS fst
LEFT JOIN GOLD.DIM_CHANNEL dch ON dch.CHANNEL_KEY = fst.CHANNEL_KEY
GROUP BY dch.CHANNEL_NAME;
```

### 9. Mobile App Engagement Funnel

```sql
SELECT
    'Product Views'      AS stage, SUM(PRODUCT_VIEW_COUNT) AS events FROM GOLD.FACT_USER_DAILY_ENGAGEMENT
UNION ALL SELECT
    'Add to Cart',       SUM(ADD_TO_CART_COUNT)        FROM GOLD.FACT_USER_DAILY_ENGAGEMENT
UNION ALL SELECT
    'Checkout Start',    SUM(CHECKOUT_START_COUNT)      FROM GOLD.FACT_USER_DAILY_ENGAGEMENT
UNION ALL SELECT
    'Checkout Complete', SUM(CHECKOUT_COMPLETE_COUNT)   FROM GOLD.FACT_USER_DAILY_ENGAGEMENT;
```

### 10. Marketing Campaign ROI

```sql
SELECT
    dc.CAMPAIGN_NAME,
    dmc.MARKETING_CHANNEL_NAME,
    dc.TARGET_SEGMENT,
    SUM(fcd.AMOUNT_SPENT)   AS total_spend,
    SUM(fcd.IMPRESSIONS)    AS total_impressions,
    SUM(fcd.CLICKS)         AS total_clicks,
    SUM(fcd.CONVERSIONS)    AS total_conversions,
    CASE WHEN SUM(fcd.CLICKS) > 0
         THEN ROUND(SUM(fcd.AMOUNT_SPENT) / SUM(fcd.CLICKS), 2)
    END AS overall_cpc,
    CASE WHEN SUM(fcd.CONVERSIONS) > 0
         THEN ROUND(SUM(fcd.AMOUNT_SPENT) / SUM(fcd.CONVERSIONS), 2)
    END AS overall_cpa
FROM GOLD.FACT_CAMPAIGN_DAILY fcd
JOIN GOLD.DIM_CAMPAIGN dc ON dc.CAMPAIGN_KEY = fcd.CAMPAIGN_KEY AND dc.IS_CURRENT = TRUE
JOIN GOLD.DIM_MARKETING_CHANNEL dmc ON dmc.MARKETING_CHANNEL_KEY = fcd.MARKETING_CHANNEL_KEY
GROUP BY dc.CAMPAIGN_NAME, dmc.MARKETING_CHANNEL_NAME, dc.TARGET_SEGMENT
ORDER BY total_spend DESC;
```

### 11. Full Order Journey (Cross-Fact)

```sql
-- Complete view of an order across all fact tables
SELECT
    fo.ORDER_ID,
    dch.CHANNEL_NAME,
    dos.STATUS_NAME,
    fo.ORDER_AMOUNT,
    -- Payment
    fp.GROSS_AMOUNT       AS payment_amount,
    dpm.PAYMENT_METHOD_NAME,
    dps.STATUS_NAME       AS payment_status,
    -- Shipment
    fs.SHIPMENT_STATUS,
    fs.DAYS_TO_DELIVER,
    fs.ON_TIME_DELIVERY_FLAG
FROM GOLD.FACT_ORDERS fo
JOIN GOLD.DIM_CHANNEL dch ON dch.CHANNEL_KEY = fo.CHANNEL_KEY
JOIN GOLD.DIM_ORDER_STATUS dos ON dos.ORDER_STATUS_KEY = fo.ORDER_STATUS_KEY
LEFT JOIN GOLD.FACT_PAYMENTS fp ON fp.ORDER_KEY = fo.ORDER_KEY
LEFT JOIN GOLD.DIM_PAYMENT_METHOD dpm ON dpm.PAYMENT_METHOD_KEY = fp.PAYMENT_METHOD_KEY
LEFT JOIN GOLD.DIM_PAYMENT_STATUS dps ON dps.PAYMENT_STATUS_KEY = fp.PAYMENT_STATUS_KEY
LEFT JOIN GOLD.FACT_SHIPMENTS fs ON fs.ORDER_KEY = fo.ORDER_KEY
WHERE fo.ORDER_ID = 8821;  -- Replace with any order ID
```

---

## Business Questions This Model Answers

### Revenue & Orders
- What is total revenue by channel, month, quarter, or year?
- What is the average order value by channel?
- How does weekend vs weekday order volume compare?
- What is the revenue split by currency?
- Which promo codes drive the most revenue?

### Products & Categories
- What are our top-selling products by revenue or units?
- Which product categories grow fastest?
- What is the return rate by product? (Web channel only)
- What is the average price point per category?
- Which products appear across the most channels?

### Customers
- Who are our highest-value customers (LTV)?
- How many customers buy across multiple channels?
- What is the customer distribution by loyalty tier?
- What is the B2B vs B2C revenue split?
- How has each customer's loyalty tier changed over time? (SCD2)

### Payments
- What is the payment success rate by method?
- Which payment methods have the highest fees?
- What are the top failure reasons?
- How does success rate differ by gateway region?

### Shipping & Fulfillment
- What is the average delivery time by warehouse?
- What is the on-time delivery percentage?
- Which facilities have the best performance?
- How does delivery performance vary by channel?

### Customer Support
- What is the average time to first reply?
- What is the average resolution time?
- What is the CSAT score by channel?
- Which agents handle the most tickets?

### Mobile App
- What is the daily active user count trend?
- What is the product view → add to cart → checkout conversion funnel?
- What is the average session duration?
- How many unique products does each user view per day?

### Marketing
- Which campaigns have the best ROI?
- What is the cost per acquisition by marketing channel?
- How do conversion rates compare across marketing channels?
- Which target segments respond best?

---

## Known Limitations & Caveats

| Area | Limitation | Impact |
|------|-----------|--------|
| **Marketplace Customers** | Amazon masks buyer emails — CUSTOMER_KEY is NULL for all 800 marketplace orders | Cannot include marketplace customers in customer LTV, segmentation, or cross-channel analysis |
| **Review ↔ Customer Link** | Reviews use anonymous REVIEWER_HANDLE — no email or customer ID available | Cannot link reviews to customer profiles or purchase history |
| **Payment Coverage** | FACT_PAYMENTS covers Web and Mobile only — Wholesale (invoicing) and Marketplace (Amazon Pay) are not in this table | Payment analysis is limited to 2 of 4 channels |
| **IS_RETURNED Flag** | Only available on Web channel order items — Mobile, Wholesale, and Marketplace do not provide return data at line-item level | Return rate analysis limited to Web channel |
| **MARKETPLACE_FEE** | Only available on Marketplace order items — NULL for all other channels | Fee analysis limited to Marketplace |
| **Mobile Customer Data** | MOB_CUSTOMERS has limited fields (missing phone, city, postal code, registration date) | Customer profiles for mobile-only users are incomplete |
| **Wholesale Customer Data** | WHL_CUSTOMERS has only email and company name — no individual contact details | B2B customer profiles are minimal |
| **Guest App Sessions** | ~15% of mobile app events are from unauthenticated sessions — CUSTOMER_KEY is NULL in FACT_USER_DAILY_ENGAGEMENT | Engagement analysis for guest users cannot be linked to customer profiles |
| **Product Master** | No brand, weight, cost, or supplier data available — DIM_PRODUCT is derived from order items | Product dimension lacks catalog-level attributes |
| **Exchange Rates** | Currently all set to 1.0 (USD baseline) — external exchange rate source not yet integrated | Multi-currency amounts are stored but USD conversion is a placeholder |

---

## Data Freshness & Refresh Schedule

### Refresh Process

The Gold layer is refreshed by running:
1. **GOLD_INCREMENTAL_LOAD_DIMENSIONS.ipynb** — Updates all 12 dimension tables
2. **GOLD_INCREMENTAL_LOAD_FACTS.ipynb** — Updates all 10 fact tables + 3 extensions

Each layer uses watermark-based incremental loading — only new data since the last run is processed.

### Watermark Column

Every fact table has a `_SILVER_LOAD_TIMESTAMP` column that tracks when the source Silver data was processed. This is the watermark for incremental loading.

### Dependencies

```
Bronze (raw files) → Silver (cleansed) → Gold (star schema)
```

Gold depends on Silver. If Silver hasn't been refreshed, Gold will process zero new rows (no impact, no errors).

---

## Glossary

| Term | Definition |
|------|-----------|
| **Surrogate Key** | Auto-generated integer key (e.g., `ORDER_KEY`, `CUSTOMER_KEY`) used for joining tables. Not meaningful on its own. |
| **Business Key** | Natural identifier from the source system (e.g., `ORDER_ID`, `PRODUCT_SKU`). Meaningful to business users. |
| **Degenerate Dimension** | A dimension attribute stored directly in the fact table rather than in a separate dimension table (e.g., `CURRENCY_CODE`, `PROMO_CODE`). Used when there's no need for a full dimension. |
| **SCD Type 2** | Slowly Changing Dimension Type 2 — historical versioning where a new row is created when tracked attributes change. Old rows are marked `IS_CURRENT = FALSE` with an `VALID_TO` date. |
| **Extension Table** | A satellite table that stores channel-specific attributes. Joined to FACT_ORDERS via `ORDER_KEY` using LEFT JOIN. |
| **Watermark** | A timestamp checkpoint tracking the last-processed record. Used for incremental loading — only records with timestamps after the watermark are processed. |
| **Canonical** | The standardized, unified version of a value that maps from multiple source system vocabularies (e.g., canonical status "completed" maps from `C`, `COMPLETE`, `ORD_FULFILLED`, `Shipped`). |
| **B2B** | Business-to-Business — wholesale channel orders from companies. |
| **B2C** | Business-to-Consumer — direct consumer orders from web, mobile, and marketplace. |
| **FBA** | Fulfilled by Amazon — Amazon handles warehousing and shipping. |
| **FBM** | Fulfilled by Merchant — seller handles warehousing and shipping. |
| **CSAT** | Customer Satisfaction score. Mapped from text labels: 5 = good, 3 = neutral, 1 = bad. |
| **LTV** | Lifetime Value — total revenue generated by a single customer over their entire history. Calculate from `SUM(ORDER_AMOUNT)` grouped by customer. |
| **Funnel** | A conversion sequence: Product View → Add to Cart → Checkout Start → Checkout Complete. Track drop-off at each stage using FACT_USER_DAILY_ENGAGEMENT. |
