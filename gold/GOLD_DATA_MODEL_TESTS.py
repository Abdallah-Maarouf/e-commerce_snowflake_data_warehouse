# ============================================================================
# GOLD LAYER — AUTOMATED DATA MODEL VALIDATION
# ============================================================================
# Purpose : Run all Gold layer data quality tests automatically and report
#           PASS / FAIL / WARN status for each test.
# Usage   : Execute in any Snowpark-connected environment (Snowflake notebook,
#           Hex, local Snowpark session, etc.)
#
# Output  : Colour-coded summary table + detail for any failures.
# ============================================================================

from snowflake.snowpark.context import get_active_session
from datetime import datetime
from collections import OrderedDict

session = get_active_session()
GOLD = 'GOLD'

# ── Result storage ──────────────────────────────────────────────────────────
results = []          # list of dicts: section, id, name, status, detail
_counters = {'pass': 0, 'fail': 0, 'warn': 0, 'error': 0}


def _run(sql):
    """Execute SQL and return list of Row objects."""
    return session.sql(sql).collect()


def _add(section, test_id, name, status, detail=''):
    """Record one test result."""
    results.append({
        'section': section,
        'id': test_id,
        'name': name,
        'status': status,
        'detail': detail,
    })
    _counters[status.lower()] += 1


# ════════════════════════════════════════════════════════════════════════════
# SECTION 1 — REFERENTIAL INTEGRITY (FK → PK orphan checks)
# ════════════════════════════════════════════════════════════════════════════
S1 = '1. Referential Integrity'

ri_tests = [
    ('1.1',  'FACT_ORDERS → DIM_ORDER_STATUS',
     f"SELECT COUNT(*) AS CNT FROM {GOLD}.FACT_ORDERS fo WHERE fo.ORDER_STATUS_KEY IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {GOLD}.DIM_ORDER_STATUS d WHERE d.ORDER_STATUS_KEY = fo.ORDER_STATUS_KEY)"),
    ('1.2',  'FACT_ORDERS → DIM_CUSTOMER',
     f"SELECT COUNT(*) AS CNT FROM {GOLD}.FACT_ORDERS fo WHERE fo.CUSTOMER_KEY IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {GOLD}.DIM_CUSTOMER d WHERE d.CUSTOMER_KEY = fo.CUSTOMER_KEY)"),
    ('1.3',  'FACT_ORDERS → DIM_DATE',
     f"SELECT COUNT(*) AS CNT FROM {GOLD}.FACT_ORDERS fo WHERE fo.ORDER_DATE_KEY IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {GOLD}.DIM_DATE d WHERE d.DATE_KEY = fo.ORDER_DATE_KEY)"),
    ('1.4',  'FACT_ORDERS → DIM_CHANNEL',
     f"SELECT COUNT(*) AS CNT FROM {GOLD}.FACT_ORDERS fo WHERE fo.CHANNEL_KEY IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {GOLD}.DIM_CHANNEL d WHERE d.CHANNEL_KEY = fo.CHANNEL_KEY)"),
    ('1.5',  'FACT_ORDER_ITEMS → FACT_ORDERS',
     f"SELECT COUNT(*) AS CNT FROM {GOLD}.FACT_ORDER_ITEMS fi WHERE fi.ORDER_KEY IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {GOLD}.FACT_ORDERS fo WHERE fo.ORDER_KEY = fi.ORDER_KEY)"),
    ('1.6',  'FACT_ORDER_ITEMS → DIM_PRODUCT',
     f"SELECT COUNT(*) AS CNT FROM {GOLD}.FACT_ORDER_ITEMS fi WHERE fi.PRODUCT_KEY IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {GOLD}.DIM_PRODUCT d WHERE d.PRODUCT_KEY = fi.PRODUCT_KEY)"),
    ('1.7',  'FACT_ORDER_ITEMS → DIM_CATEGORY',
     f"SELECT COUNT(*) AS CNT FROM {GOLD}.FACT_ORDER_ITEMS fi WHERE fi.CATEGORY_KEY IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {GOLD}.DIM_CATEGORY d WHERE d.CATEGORY_KEY = fi.CATEGORY_KEY)"),
    ('1.8',  'FACT_PAYMENTS → DIM_PAYMENT_METHOD',
     f"SELECT COUNT(*) AS CNT FROM {GOLD}.FACT_PAYMENTS fp WHERE fp.PAYMENT_METHOD_KEY IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {GOLD}.DIM_PAYMENT_METHOD d WHERE d.PAYMENT_METHOD_KEY = fp.PAYMENT_METHOD_KEY)"),
    ('1.9',  'FACT_PAYMENTS → DIM_PAYMENT_STATUS',
     f"SELECT COUNT(*) AS CNT FROM {GOLD}.FACT_PAYMENTS fp WHERE fp.PAYMENT_STATUS_KEY IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {GOLD}.DIM_PAYMENT_STATUS d WHERE d.PAYMENT_STATUS_KEY = fp.PAYMENT_STATUS_KEY)"),
    ('1.10', 'FACT_PAYMENTS → FACT_ORDERS',
     f"SELECT COUNT(*) AS CNT FROM {GOLD}.FACT_PAYMENTS fp WHERE fp.ORDER_KEY IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {GOLD}.FACT_ORDERS fo WHERE fo.ORDER_KEY = fp.ORDER_KEY)"),
    ('1.11', 'FACT_SHIPMENTS → FACT_ORDERS',
     f"SELECT COUNT(*) AS CNT FROM {GOLD}.FACT_SHIPMENTS fs WHERE fs.ORDER_KEY IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {GOLD}.FACT_ORDERS fo WHERE fo.ORDER_KEY = fs.ORDER_KEY)"),
    ('1.12', 'FACT_REVIEWS → DIM_PRODUCT',
     f"SELECT COUNT(*) AS CNT FROM {GOLD}.FACT_REVIEWS fr WHERE fr.PRODUCT_KEY IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {GOLD}.DIM_PRODUCT d WHERE d.PRODUCT_KEY = fr.PRODUCT_KEY)"),
    ('1.13', 'FACT_SUPPORT_TICKETS → FACT_ORDERS',
     f"SELECT COUNT(*) AS CNT FROM {GOLD}.FACT_SUPPORT_TICKETS st WHERE st.ORDER_KEY IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {GOLD}.FACT_ORDERS fo WHERE fo.ORDER_KEY = st.ORDER_KEY)"),
    ('1.14', 'FACT_SUPPORT_TICKETS → DIM_CUSTOMER',
     f"SELECT COUNT(*) AS CNT FROM {GOLD}.FACT_SUPPORT_TICKETS st WHERE st.CUSTOMER_KEY IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {GOLD}.DIM_CUSTOMER d WHERE d.CUSTOMER_KEY = st.CUSTOMER_KEY)"),
    ('1.15', 'FACT_USER_DAILY_ENGAGEMENT → DIM_CUSTOMER',
     f"SELECT COUNT(*) AS CNT FROM {GOLD}.FACT_USER_DAILY_ENGAGEMENT u WHERE u.CUSTOMER_KEY IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {GOLD}.DIM_CUSTOMER d WHERE d.CUSTOMER_KEY = u.CUSTOMER_KEY)"),
    ('1.16', 'FACT_CAMPAIGN_DAILY → DIM_CAMPAIGN',
     f"SELECT COUNT(*) AS CNT FROM {GOLD}.FACT_CAMPAIGN_DAILY fc WHERE fc.CAMPAIGN_KEY IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {GOLD}.DIM_CAMPAIGN d WHERE d.CAMPAIGN_KEY = fc.CAMPAIGN_KEY)"),
    ('1.17', 'FACT_CAMPAIGN_DAILY → DIM_MARKETING_CHANNEL',
     f"SELECT COUNT(*) AS CNT FROM {GOLD}.FACT_CAMPAIGN_DAILY fc WHERE fc.MARKETING_CHANNEL_KEY IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {GOLD}.DIM_MARKETING_CHANNEL d WHERE d.MARKETING_CHANNEL_KEY = fc.MARKETING_CHANNEL_KEY)"),
]

# Extension tables (each returns one count)
ri_ext_tests = [
    ('1.18a', 'MOBILE_EXT → FACT_ORDERS',
     f"SELECT COUNT(*) AS CNT FROM {GOLD}.FACT_ORDERS_MOBILE_EXT e WHERE NOT EXISTS (SELECT 1 FROM {GOLD}.FACT_ORDERS fo WHERE fo.ORDER_KEY = e.ORDER_KEY)"),
    ('1.18b', 'WHOLESALE_EXT → FACT_ORDERS',
     f"SELECT COUNT(*) AS CNT FROM {GOLD}.FACT_ORDERS_WHOLESALE_EXT e WHERE NOT EXISTS (SELECT 1 FROM {GOLD}.FACT_ORDERS fo WHERE fo.ORDER_KEY = e.ORDER_KEY)"),
    ('1.18c', 'MARKETPLACE_EXT → FACT_ORDERS',
     f"SELECT COUNT(*) AS CNT FROM {GOLD}.FACT_ORDERS_MARKETPLACE_EXT e WHERE NOT EXISTS (SELECT 1 FROM {GOLD}.FACT_ORDERS fo WHERE fo.ORDER_KEY = e.ORDER_KEY)"),
]

print(f'{"="*70}')
print(f'  SECTION 1 — Referential Integrity (FK → PK orphan checks)')
print(f'{"="*70}')

for tid, name, sql in ri_tests + ri_ext_tests:
    try:
        cnt = _run(sql)[0]['CNT']
        if cnt == 0:
            _add(S1, tid, name, 'PASS')
            print(f'  ✓ PASS  {tid:6s}  {name}')
        else:
            _add(S1, tid, name, 'FAIL', f'{cnt:,} orphan FK rows')
            print(f'  ✗ FAIL  {tid:6s}  {name}  →  {cnt:,} orphan rows')
    except Exception as e:
        _add(S1, tid, name, 'ERROR', str(e)[:120])
        print(f'  ⚠ ERROR {tid:6s}  {name}  →  {str(e)[:80]}')


# ════════════════════════════════════════════════════════════════════════════
# SECTION 2 — NULL FK COVERAGE
# ════════════════════════════════════════════════════════════════════════════
S2 = '2. NULL FK Coverage'
print(f'\n{"="*70}')
print(f'  SECTION 2 — NULL FK Coverage')
print(f'{"="*70}')

# --- 2.1 FACT_ORDERS ---
try:
    r = _run(f'''
        SELECT
            COUNT(*)                                      AS TOTAL,
            SUM(IFF(ORDER_STATUS_KEY IS NULL, 1, 0))       AS NULL_STATUS,
            SUM(IFF(ORDER_DATE_KEY   IS NULL, 1, 0))       AS NULL_DATE,
            SUM(IFF(CHANNEL_KEY      IS NULL, 1, 0))       AS NULL_CHANNEL,
            SUM(IFF(CUSTOMER_KEY     IS NULL, 1, 0))       AS NULL_CUST
        FROM {GOLD}.FACT_ORDERS
    ''')[0]
    total = r['TOTAL']

    # STATUS — expect 0%
    pct = round(r['NULL_STATUS'] / total * 100, 1) if total else 0
    if r['NULL_STATUS'] == 0:
        _add(S2, '2.1a', 'FACT_ORDERS: NULL ORDER_STATUS_KEY', 'PASS', f'0 / {total:,}')
        print(f'  ✓ PASS  2.1a    NULL STATUS_KEY: 0 / {total:,} (0%)')
    else:
        _add(S2, '2.1a', 'FACT_ORDERS: NULL ORDER_STATUS_KEY', 'FAIL', f'{r["NULL_STATUS"]:,} / {total:,} ({pct}%)')
        print(f'  ✗ FAIL  2.1a    NULL STATUS_KEY: {r["NULL_STATUS"]:,} / {total:,} ({pct}%)')

    # DATE — expect 0%
    pct = round(r['NULL_DATE'] / total * 100, 1) if total else 0
    if r['NULL_DATE'] == 0:
        _add(S2, '2.1b', 'FACT_ORDERS: NULL ORDER_DATE_KEY', 'PASS', f'0 / {total:,}')
        print(f'  ✓ PASS  2.1b    NULL DATE_KEY: 0 / {total:,} (0%)')
    else:
        _add(S2, '2.1b', 'FACT_ORDERS: NULL ORDER_DATE_KEY', 'FAIL', f'{r["NULL_DATE"]:,} / {total:,} ({pct}%)')
        print(f'  ✗ FAIL  2.1b    NULL DATE_KEY: {r["NULL_DATE"]:,} / {total:,} ({pct}%)')

    # CHANNEL — expect 0%
    pct = round(r['NULL_CHANNEL'] / total * 100, 1) if total else 0
    if r['NULL_CHANNEL'] == 0:
        _add(S2, '2.1c', 'FACT_ORDERS: NULL CHANNEL_KEY', 'PASS', f'0 / {total:,}')
        print(f'  ✓ PASS  2.1c    NULL CHANNEL_KEY: 0 / {total:,} (0%)')
    else:
        _add(S2, '2.1c', 'FACT_ORDERS: NULL CHANNEL_KEY', 'FAIL', f'{r["NULL_CHANNEL"]:,} / {total:,} ({pct}%)')
        print(f'  ✗ FAIL  2.1c    NULL CHANNEL_KEY: {r["NULL_CHANNEL"]:,} / {total:,} ({pct}%)')

    # CUSTOMER — expect ~10% (marketplace); warn if > 25%
    pct = round(r['NULL_CUST'] / total * 100, 1) if total else 0
    if pct <= 25:
        _add(S2, '2.1d', 'FACT_ORDERS: NULL CUSTOMER_KEY', 'PASS', f'{r["NULL_CUST"]:,} / {total:,} ({pct}%) — marketplace orders expected')
        print(f'  ✓ PASS  2.1d    NULL CUSTOMER_KEY: {r["NULL_CUST"]:,} / {total:,} ({pct}%) [marketplace expected ≈10%]')
    else:
        _add(S2, '2.1d', 'FACT_ORDERS: NULL CUSTOMER_KEY', 'WARN', f'{r["NULL_CUST"]:,} / {total:,} ({pct}%) — higher than expected')
        print(f'  ⚠ WARN  2.1d    NULL CUSTOMER_KEY: {r["NULL_CUST"]:,} / {total:,} ({pct}%) [expected ≈10%, got {pct}%]')
except Exception as e:
    _add(S2, '2.1', 'FACT_ORDERS NULL keys', 'ERROR', str(e)[:120])
    print(f'  ⚠ ERROR 2.1     FACT_ORDERS  →  {str(e)[:80]}')

# --- 2.2 FACT_ORDER_ITEMS ---
try:
    r = _run(f'''
        SELECT
            COUNT(*)                                   AS TOTAL,
            SUM(IFF(ORDER_KEY    IS NULL, 1, 0))        AS NULL_ORDER,
            SUM(IFF(PRODUCT_KEY  IS NULL, 1, 0))        AS NULL_PRODUCT,
            SUM(IFF(CATEGORY_KEY IS NULL, 1, 0))        AS NULL_CATEGORY,
            SUM(IFF(CHANNEL_KEY  IS NULL, 1, 0))        AS NULL_CHANNEL
        FROM {GOLD}.FACT_ORDER_ITEMS
    ''')[0]
    total = r['TOTAL']
    for col_label, col_key, tid in [
        ('ORDER_KEY',    'NULL_ORDER',    '2.2a'),
        ('PRODUCT_KEY',  'NULL_PRODUCT',  '2.2b'),
        ('CATEGORY_KEY', 'NULL_CATEGORY', '2.2c'),
        ('CHANNEL_KEY',  'NULL_CHANNEL',  '2.2d'),
    ]:
        cnt = r[col_key]
        pct = round(cnt / total * 100, 1) if total else 0
        if cnt == 0:
            _add(S2, tid, f'FACT_ORDER_ITEMS: NULL {col_label}', 'PASS', f'0 / {total:,}')
            print(f'  ✓ PASS  {tid}    NULL {col_label}: 0 / {total:,}')
        else:
            _add(S2, tid, f'FACT_ORDER_ITEMS: NULL {col_label}', 'FAIL', f'{cnt:,} / {total:,} ({pct}%)')
            print(f'  ✗ FAIL  {tid}    NULL {col_label}: {cnt:,} / {total:,} ({pct}%)')
except Exception as e:
    _add(S2, '2.2', 'FACT_ORDER_ITEMS NULL keys', 'ERROR', str(e)[:120])
    print(f'  ⚠ ERROR 2.2     FACT_ORDER_ITEMS  →  {str(e)[:80]}')

# --- 2.3 FACT_PAYMENTS ---
try:
    r = _run(f'''
        SELECT
            COUNT(*)                                       AS TOTAL,
            SUM(IFF(ORDER_KEY          IS NULL, 1, 0))      AS NULL_ORDER,
            SUM(IFF(PAYMENT_METHOD_KEY IS NULL, 1, 0))      AS NULL_METHOD,
            SUM(IFF(PAYMENT_STATUS_KEY IS NULL, 1, 0))      AS NULL_STATUS,
            SUM(IFF(CHANNEL_KEY        IS NULL, 1, 0))      AS NULL_CHANNEL
        FROM {GOLD}.FACT_PAYMENTS
    ''')[0]
    total = r['TOTAL']

    # ORDER_KEY — expect ~8% (abandoned checkouts)
    cnt = r['NULL_ORDER']
    pct = round(cnt / total * 100, 1) if total else 0
    if pct <= 15:
        _add(S2, '2.3a', 'FACT_PAYMENTS: NULL ORDER_KEY', 'PASS', f'{cnt:,} / {total:,} ({pct}%) — abandoned checkouts expected ≈8%')
        print(f'  ✓ PASS  2.3a    NULL ORDER_KEY: {cnt:,} / {total:,} ({pct}%) [abandoned checkouts ≈8%]')
    else:
        _add(S2, '2.3a', 'FACT_PAYMENTS: NULL ORDER_KEY', 'WARN', f'{cnt:,} / {total:,} ({pct}%)')
        print(f'  ⚠ WARN  2.3a    NULL ORDER_KEY: {cnt:,} / {total:,} ({pct}%) [expected ≈8%]')

    # METHOD, STATUS, CHANNEL — expect 0
    for col_label, col_key, tid in [
        ('PAYMENT_METHOD_KEY', 'NULL_METHOD',  '2.3b'),
        ('PAYMENT_STATUS_KEY', 'NULL_STATUS',  '2.3c'),
        ('CHANNEL_KEY',        'NULL_CHANNEL', '2.3d'),
    ]:
        cnt = r[col_key]
        pct = round(cnt / total * 100, 1) if total else 0
        if cnt == 0:
            _add(S2, tid, f'FACT_PAYMENTS: NULL {col_label}', 'PASS', f'0 / {total:,}')
            print(f'  ✓ PASS  {tid}    NULL {col_label}: 0 / {total:,}')
        else:
            _add(S2, tid, f'FACT_PAYMENTS: NULL {col_label}', 'FAIL', f'{cnt:,} / {total:,} ({pct}%)')
            print(f'  ✗ FAIL  {tid}    NULL {col_label}: {cnt:,} / {total:,} ({pct}%)')
except Exception as e:
    _add(S2, '2.3', 'FACT_PAYMENTS NULL keys', 'ERROR', str(e)[:120])
    print(f'  ⚠ ERROR 2.3     FACT_PAYMENTS  →  {str(e)[:80]}')


# ════════════════════════════════════════════════════════════════════════════
# SECTION 3 — ANALYTICAL JOIN TESTS
# Verify multi-table star schema joins return rows (non-zero = working)
# ════════════════════════════════════════════════════════════════════════════
S3 = '3. Analytical Joins'
print(f'\n{"="*70}')
print(f'  SECTION 3 — Analytical Join Tests')
print(f'{"="*70}')

join_tests = [
    ('3.1', 'Revenue by Channel × Status',
     f"""SELECT COUNT(*) AS CNT FROM (
         SELECT dch.CHANNEL_NAME, dos.STATUS_NAME, COUNT(*) AS c
         FROM {GOLD}.FACT_ORDERS fo
         JOIN {GOLD}.DIM_CHANNEL dch ON dch.CHANNEL_KEY = fo.CHANNEL_KEY
         JOIN {GOLD}.DIM_ORDER_STATUS dos ON dos.ORDER_STATUS_KEY = fo.ORDER_STATUS_KEY
         GROUP BY dch.CHANNEL_NAME, dos.STATUS_NAME
     )""",
     4, 'Expected ≥4 channel×status combos (4 channels × ≥1 status)'),

    ('3.2', 'Monthly Revenue by Channel (DIM_DATE join)',
     f"""SELECT COUNT(*) AS CNT FROM (
         SELECT dd.YEAR, dd.MONTH, dch.CHANNEL_NAME, COUNT(*) AS c
         FROM {GOLD}.FACT_ORDERS fo
         JOIN {GOLD}.DIM_DATE dd ON dd.DATE_KEY = fo.ORDER_DATE_KEY
         JOIN {GOLD}.DIM_CHANNEL dch ON dch.CHANNEL_KEY = fo.CHANNEL_KEY
         GROUP BY dd.YEAR, dd.MONTH, dch.CHANNEL_NAME
     )""",
     4, 'Expected ≥4 month×channel combos'),

    ('3.3', 'Top Products (4-way: Items→Product→Category→Channel)',
     f"""SELECT COUNT(*) AS CNT FROM (
         SELECT dp.PRODUCT_SKU, dc.CATEGORY_NAME, dch.CHANNEL_NAME, SUM(foi.LINE_TOTAL) AS rev
         FROM {GOLD}.FACT_ORDER_ITEMS foi
         JOIN {GOLD}.DIM_PRODUCT dp ON dp.PRODUCT_KEY = foi.PRODUCT_KEY AND dp.IS_CURRENT = TRUE
         JOIN {GOLD}.DIM_CATEGORY dc ON dc.CATEGORY_KEY = foi.CATEGORY_KEY
         JOIN {GOLD}.DIM_CHANNEL dch ON dch.CHANNEL_KEY = foi.CHANNEL_KEY
         GROUP BY dp.PRODUCT_SKU, dc.CATEGORY_NAME, dch.CHANNEL_NAME
     )""",
     10, 'Expected ≥10 product×category×channel combos'),

    ('3.4', 'Customer Lifetime Value (DIM_CUSTOMER SCD2 IS_CURRENT)',
     f"""SELECT COUNT(*) AS CNT FROM (
         SELECT dc.CUSTOMER_ID, SUM(fo.ORDER_AMOUNT) AS ltv
         FROM {GOLD}.FACT_ORDERS fo
         JOIN {GOLD}.DIM_CUSTOMER dc ON dc.CUSTOMER_KEY = fo.CUSTOMER_KEY AND dc.IS_CURRENT = TRUE
         GROUP BY dc.CUSTOMER_ID
     )""",
     10, 'Expected ≥10 customers with orders'),

    ('3.5', 'Order→Payment cross-fact join',
     f"""SELECT COUNT(*) AS CNT
         FROM {GOLD}.FACT_ORDERS fo
         JOIN {GOLD}.FACT_PAYMENTS fp ON fp.ORDER_KEY = fo.ORDER_KEY
         JOIN {GOLD}.DIM_PAYMENT_METHOD dpm ON dpm.PAYMENT_METHOD_KEY = fp.PAYMENT_METHOD_KEY
         JOIN {GOLD}.DIM_PAYMENT_STATUS dps ON dps.PAYMENT_STATUS_KEY = fp.PAYMENT_STATUS_KEY""",
     100, 'Expected ≥100 orders with payment + method + status'),

    ('3.6', 'Order→Shipment cross-fact join',
     f"""SELECT COUNT(*) AS CNT
         FROM {GOLD}.FACT_ORDERS fo
         JOIN {GOLD}.FACT_SHIPMENTS fs ON fs.ORDER_KEY = fo.ORDER_KEY
         JOIN {GOLD}.DIM_CHANNEL dch ON dch.CHANNEL_KEY = fo.CHANNEL_KEY""",
     100, 'Expected ≥100 orders with shipments'),

    ('3.7', 'Full Order Journey (4 facts: Orders+Items+Payments+Shipments)',
     f"""SELECT COUNT(*) AS CNT FROM (
         SELECT fo.ORDER_ID, dch.CHANNEL_NAME, dos.STATUS_NAME
         FROM {GOLD}.FACT_ORDERS fo
         JOIN {GOLD}.DIM_CHANNEL dch ON dch.CHANNEL_KEY = fo.CHANNEL_KEY
         JOIN {GOLD}.DIM_ORDER_STATUS dos ON dos.ORDER_STATUS_KEY = fo.ORDER_STATUS_KEY
         LEFT JOIN {GOLD}.FACT_ORDER_ITEMS foi ON foi.ORDER_KEY = fo.ORDER_KEY
         LEFT JOIN {GOLD}.FACT_PAYMENTS fp ON fp.ORDER_KEY = fo.ORDER_KEY
         LEFT JOIN {GOLD}.FACT_SHIPMENTS fs ON fs.ORDER_KEY = fo.ORDER_KEY
         GROUP BY fo.ORDER_ID, dch.CHANNEL_NAME, dos.STATUS_NAME
     )""",
     100, 'Expected ≥100 orders resolved across all 4 fact tables'),

    ('3.8', 'Support Tickets with Customer + Order context',
     f"""SELECT COUNT(*) AS CNT
         FROM {GOLD}.FACT_SUPPORT_TICKETS fst
         LEFT JOIN {GOLD}.DIM_CUSTOMER dc ON dc.CUSTOMER_KEY = fst.CUSTOMER_KEY AND dc.IS_CURRENT = TRUE
         LEFT JOIN {GOLD}.FACT_ORDERS fo ON fo.ORDER_KEY = fst.ORDER_KEY""",
     50, 'Expected ≥50 support tickets'),

    ('3.9', 'Reviews → Product + Category + Date',
     f"""SELECT COUNT(*) AS CNT
         FROM {GOLD}.FACT_REVIEWS fr
         JOIN {GOLD}.DIM_PRODUCT dp ON dp.PRODUCT_KEY = fr.PRODUCT_KEY AND dp.IS_CURRENT = TRUE
         JOIN {GOLD}.DIM_CATEGORY dc ON dc.CATEGORY_KEY = fr.CATEGORY_KEY
         JOIN {GOLD}.DIM_DATE dd ON dd.DATE_KEY = fr.REVIEW_DATE_KEY""",
     50, 'Expected ≥50 reviews with product+category+date'),

    ('3.10', 'User Daily Engagement → Customer + Date',
     f"""SELECT COUNT(*) AS CNT
         FROM {GOLD}.FACT_USER_DAILY_ENGAGEMENT fude
         LEFT JOIN {GOLD}.DIM_CUSTOMER dc ON dc.CUSTOMER_KEY = fude.CUSTOMER_KEY AND dc.IS_CURRENT = TRUE
         JOIN {GOLD}.DIM_DATE dd ON dd.DATE_KEY = fude.ACTIVITY_DATE_KEY""",
     100, 'Expected ≥100 engagement rows with date'),

    ('3.11', 'Campaign Daily → Campaign + MarketingChannel + Date',
     f"""SELECT COUNT(*) AS CNT
         FROM {GOLD}.FACT_CAMPAIGN_DAILY fcd
         JOIN {GOLD}.DIM_CAMPAIGN dc ON dc.CAMPAIGN_KEY = fcd.CAMPAIGN_KEY AND dc.IS_CURRENT = TRUE
         JOIN {GOLD}.DIM_MARKETING_CHANNEL dmc ON dmc.MARKETING_CHANNEL_KEY = fcd.MARKETING_CHANNEL_KEY
         JOIN {GOLD}.DIM_DATE dd ON dd.DATE_KEY = fcd.DATE_KEY""",
     50, 'Expected ≥50 campaign daily rows'),
]

for tid, name, sql, min_rows, note in join_tests:
    try:
        cnt = _run(sql)[0]['CNT']
        if cnt >= min_rows:
            _add(S3, tid, name, 'PASS', f'{cnt:,} rows ({note})')
            print(f'  ✓ PASS  {tid:6s}  {name}: {cnt:,} rows')
        elif cnt > 0:
            _add(S3, tid, name, 'WARN', f'{cnt:,} rows — lower than expected ({note})')
            print(f'  ⚠ WARN  {tid:6s}  {name}: {cnt:,} rows (expected ≥{min_rows})')
        else:
            _add(S3, tid, name, 'FAIL', f'0 rows — join returned nothing ({note})')
            print(f'  ✗ FAIL  {tid:6s}  {name}: 0 rows — join is broken')
    except Exception as e:
        _add(S3, tid, name, 'ERROR', str(e)[:120])
        print(f'  ⚠ ERROR {tid:6s}  {name}  →  {str(e)[:80]}')


# ════════════════════════════════════════════════════════════════════════════
# SECTION 4 — EXTENSION TABLE JOINS
# ════════════════════════════════════════════════════════════════════════════
S4 = '4. Extension Tables'
print(f'\n{"="*70}')
print(f'  SECTION 4 — Extension Table Joins')
print(f'{"="*70}')

ext_tests = [
    ('4.1', 'Mobile EXT: all rows are Mobile App channel',
     f"""SELECT
            SUM(IFF(dch.CHANNEL_NAME = 'Mobile App', 1, 0)) AS CORRECT,
            SUM(IFF(dch.CHANNEL_NAME != 'Mobile App', 1, 0)) AS WRONG,
            COUNT(*) AS TOTAL
         FROM {GOLD}.FACT_ORDERS fo
         JOIN {GOLD}.FACT_ORDERS_MOBILE_EXT mext ON mext.ORDER_KEY = fo.ORDER_KEY
         JOIN {GOLD}.DIM_CHANNEL dch ON dch.CHANNEL_KEY = fo.CHANNEL_KEY""",
     'WRONG', 'Mobile App'),

    ('4.2', 'Wholesale EXT: all rows are Wholesale Portal channel',
     f"""SELECT
            SUM(IFF(dch.CHANNEL_NAME = 'Wholesale Portal', 1, 0)) AS CORRECT,
            SUM(IFF(dch.CHANNEL_NAME != 'Wholesale Portal', 1, 0)) AS WRONG,
            COUNT(*) AS TOTAL
         FROM {GOLD}.FACT_ORDERS fo
         JOIN {GOLD}.FACT_ORDERS_WHOLESALE_EXT wext ON wext.ORDER_KEY = fo.ORDER_KEY
         JOIN {GOLD}.DIM_CHANNEL dch ON dch.CHANNEL_KEY = fo.CHANNEL_KEY""",
     'WRONG', 'Wholesale Portal'),

    ('4.3', 'Marketplace EXT: all rows are Marketplace channel',
     f"""SELECT
            SUM(IFF(dch.CHANNEL_NAME = 'Marketplace', 1, 0)) AS CORRECT,
            SUM(IFF(dch.CHANNEL_NAME != 'Marketplace', 1, 0)) AS WRONG,
            COUNT(*) AS TOTAL
         FROM {GOLD}.FACT_ORDERS fo
         JOIN {GOLD}.FACT_ORDERS_MARKETPLACE_EXT mkext ON mkext.ORDER_KEY = fo.ORDER_KEY
         JOIN {GOLD}.DIM_CHANNEL dch ON dch.CHANNEL_KEY = fo.CHANNEL_KEY""",
     'WRONG', 'Marketplace'),

    ('4.4', 'Marketplace EXT: CUSTOMER_KEY always NULL',
     f"""SELECT
            SUM(IFF(fo.CUSTOMER_KEY IS NULL, 1, 0)) AS CORRECT,
            SUM(IFF(fo.CUSTOMER_KEY IS NOT NULL, 1, 0)) AS WRONG,
            COUNT(*) AS TOTAL
         FROM {GOLD}.FACT_ORDERS fo
         JOIN {GOLD}.FACT_ORDERS_MARKETPLACE_EXT mkext ON mkext.ORDER_KEY = fo.ORDER_KEY""",
     'WRONG', 'NULL CUSTOMER_KEY for marketplace'),
]

for tid, name, sql, wrong_col, expected_val in ext_tests:
    try:
        r = _run(sql)[0]
        total = r['TOTAL']
        wrong = r[wrong_col]
        if total == 0:
            _add(S4, tid, name, 'WARN', 'Extension table is empty')
            print(f'  ⚠ WARN  {tid:6s}  {name}: empty (0 rows)')
        elif wrong == 0:
            _add(S4, tid, name, 'PASS', f'{total:,} rows, all {expected_val}')
            print(f'  ✓ PASS  {tid:6s}  {name}: {total:,} rows OK')
        else:
            _add(S4, tid, name, 'FAIL', f'{wrong:,} / {total:,} rows have wrong channel')
            print(f'  ✗ FAIL  {tid:6s}  {name}: {wrong:,} / {total:,} rows wrong')
    except Exception as e:
        _add(S4, tid, name, 'ERROR', str(e)[:120])
        print(f'  ⚠ ERROR {tid:6s}  {name}  →  {str(e)[:80]}')


# ════════════════════════════════════════════════════════════════════════════
# SECTION 5 — DATA CONSISTENCY & BUSINESS RULES
# ════════════════════════════════════════════════════════════════════════════
S5 = '5. Data Consistency'
print(f'\n{"="*70}')
print(f'  SECTION 5 — Data Consistency & Business Rules')
print(f'{"="*70}')

# --- 5.1 Extension coverage per channel ---
try:
    rows = _run(f'''
        SELECT
            dch.CHANNEL_NAME,
            COUNT(DISTINCT fo.ORDER_KEY) AS ORDERS_IN_FACT,
            COUNT(DISTINCT mext.ORDER_KEY) AS IN_MOBILE,
            COUNT(DISTINCT wext.ORDER_KEY) AS IN_WHOLESALE,
            COUNT(DISTINCT mkext.ORDER_KEY) AS IN_MARKETPLACE
        FROM {GOLD}.FACT_ORDERS fo
        JOIN {GOLD}.DIM_CHANNEL dch ON dch.CHANNEL_KEY = fo.CHANNEL_KEY
        LEFT JOIN {GOLD}.FACT_ORDERS_MOBILE_EXT mext ON mext.ORDER_KEY = fo.ORDER_KEY
        LEFT JOIN {GOLD}.FACT_ORDERS_WHOLESALE_EXT wext ON wext.ORDER_KEY = fo.ORDER_KEY
        LEFT JOIN {GOLD}.FACT_ORDERS_MARKETPLACE_EXT mkext ON mkext.ORDER_KEY = fo.ORDER_KEY
        GROUP BY dch.CHANNEL_NAME
    ''')
    issues = []
    for r in rows:
        ch = r['CHANNEL_NAME']
        total = r['ORDERS_IN_FACT']
        mob, whl, mkt = r['IN_MOBILE'], r['IN_WHOLESALE'], r['IN_MARKETPLACE']
        if ch == 'Mobile App' and mob != total:
            issues.append(f'Mobile: {mob}/{total} in ext')
        if ch == 'Wholesale Portal' and whl != total:
            issues.append(f'Wholesale: {whl}/{total} in ext')
        if ch == 'Marketplace' and mkt != total:
            issues.append(f'Marketplace: {mkt}/{total} in ext')
        # Non-matching channels should have 0
        if ch == 'Web Store' and (mob + whl + mkt) > 0:
            issues.append(f'Web Store has {mob+whl+mkt} extension rows (should be 0)')
    if not issues:
        _add(S5, '5.1', 'Extension table coverage per channel', 'PASS', 'All channels match')
        print(f'  ✓ PASS  5.1     Extension coverage: all channels match their extension tables')
    else:
        _add(S5, '5.1', 'Extension table coverage per channel', 'FAIL', '; '.join(issues))
        print(f'  ✗ FAIL  5.1     Extension coverage: {"; ".join(issues)}')
except Exception as e:
    _add(S5, '5.1', 'Extension table coverage', 'ERROR', str(e)[:120])
    print(f'  ⚠ ERROR 5.1     Extension coverage  →  {str(e)[:80]}')

# --- 5.2 Order amount vs line item totals ---
try:
    cnt = _run(f'''
        SELECT COUNT(*) AS CNT FROM (
            SELECT fo.ORDER_ID
            FROM {GOLD}.FACT_ORDERS fo
            JOIN {GOLD}.FACT_ORDER_ITEMS foi ON foi.ORDER_KEY = fo.ORDER_KEY
            JOIN {GOLD}.DIM_CHANNEL dch ON dch.CHANNEL_KEY = fo.CHANNEL_KEY
            WHERE dch.CHANNEL_NAME != 'Wholesale Portal'
            GROUP BY fo.ORDER_ID, fo.ORDER_AMOUNT
            HAVING ABS(fo.ORDER_AMOUNT - SUM(foi.LINE_TOTAL)) > 1.00
        )
    ''')[0]['CNT']
    if cnt == 0:
        _add(S5, '5.2', 'Order amount = sum(line items) [excl wholesale tax]', 'PASS')
        print(f'  ✓ PASS  5.2     Order amount matches line item totals (excl wholesale)')
    elif cnt <= 50:
        _add(S5, '5.2', 'Order amount vs line items', 'WARN', f'{cnt:,} orders differ by >$1')
        print(f'  ⚠ WARN  5.2     {cnt:,} orders differ by >$1 (minor rounding)')
    else:
        _add(S5, '5.2', 'Order amount vs line items', 'FAIL', f'{cnt:,} orders differ by >$1')
        print(f'  ✗ FAIL  5.2     {cnt:,} orders have amount mismatch >$1')
except Exception as e:
    _add(S5, '5.2', 'Order amount vs line items', 'ERROR', str(e)[:120])
    print(f'  ⚠ ERROR 5.2     Amount consistency  →  {str(e)[:80]}')

# --- 5.3 DIM_CUSTOMER SCD2: one current per customer ---
try:
    cnt = _run(f'''
        SELECT COUNT(*) AS CNT FROM (
            SELECT CUSTOMER_ID
            FROM {GOLD}.DIM_CUSTOMER
            WHERE IS_CURRENT = TRUE
            GROUP BY CUSTOMER_ID
            HAVING COUNT(*) > 1
        )
    ''')[0]['CNT']
    if cnt == 0:
        _add(S5, '5.3', 'DIM_CUSTOMER SCD2: 1 current row per customer', 'PASS')
        print(f'  ✓ PASS  5.3     DIM_CUSTOMER SCD2: exactly 1 current row per customer')
    else:
        _add(S5, '5.3', 'DIM_CUSTOMER SCD2: duplicate current rows', 'FAIL', f'{cnt:,} customers have >1 current row')
        print(f'  ✗ FAIL  5.3     DIM_CUSTOMER SCD2: {cnt:,} customers have multiple IS_CURRENT=TRUE rows')
except Exception as e:
    _add(S5, '5.3', 'DIM_CUSTOMER SCD2', 'ERROR', str(e)[:120])
    print(f'  ⚠ ERROR 5.3     DIM_CUSTOMER SCD2  →  {str(e)[:80]}')

# --- 5.4 DIM_PRODUCT SCD2: one current per SKU ---
try:
    cnt = _run(f'''
        SELECT COUNT(*) AS CNT FROM (
            SELECT PRODUCT_SKU
            FROM {GOLD}.DIM_PRODUCT
            WHERE IS_CURRENT = TRUE
            GROUP BY PRODUCT_SKU
            HAVING COUNT(*) > 1
        )
    ''')[0]['CNT']
    if cnt == 0:
        _add(S5, '5.4', 'DIM_PRODUCT SCD2: 1 current row per SKU', 'PASS')
        print(f'  ✓ PASS  5.4     DIM_PRODUCT SCD2: exactly 1 current row per SKU')
    else:
        _add(S5, '5.4', 'DIM_PRODUCT SCD2: duplicate current rows', 'FAIL', f'{cnt:,} SKUs have >1 current row')
        print(f'  ✗ FAIL  5.4     DIM_PRODUCT SCD2: {cnt:,} SKUs have multiple IS_CURRENT=TRUE rows')
except Exception as e:
    _add(S5, '5.4', 'DIM_PRODUCT SCD2', 'ERROR', str(e)[:120])
    print(f'  ⚠ ERROR 5.4     DIM_PRODUCT SCD2  →  {str(e)[:80]}')

# --- 5.5 FACT_ORDERS: no duplicate ORDER_ID + CHANNEL ---
try:
    cnt = _run(f'''
        SELECT COUNT(*) AS CNT FROM (
            SELECT ORDER_ID, CHANNEL_KEY
            FROM {GOLD}.FACT_ORDERS
            GROUP BY ORDER_ID, CHANNEL_KEY
            HAVING COUNT(*) > 1
        )
    ''')[0]['CNT']
    if cnt == 0:
        _add(S5, '5.5', 'FACT_ORDERS: no duplicate ORDER_ID+CHANNEL', 'PASS')
        print(f'  ✓ PASS  5.5     FACT_ORDERS: no duplicate ORDER_ID+CHANNEL pairs')
    else:
        _add(S5, '5.5', 'FACT_ORDERS: duplicate ORDER_ID+CHANNEL', 'FAIL', f'{cnt:,} duplicate pairs')
        print(f'  ✗ FAIL  5.5     FACT_ORDERS: {cnt:,} duplicate ORDER_ID+CHANNEL pairs')
except Exception as e:
    _add(S5, '5.5', 'FACT_ORDERS duplicates', 'ERROR', str(e)[:120])
    print(f'  ⚠ ERROR 5.5     FACT_ORDERS dupes  →  {str(e)[:80]}')

# --- 5.6 FACT_CAMPAIGN_DAILY: no duplicate CAMPAIGN_KEY + DATE_KEY ---
try:
    cnt = _run(f'''
        SELECT COUNT(*) AS CNT FROM (
            SELECT CAMPAIGN_KEY, DATE_KEY
            FROM {GOLD}.FACT_CAMPAIGN_DAILY
            GROUP BY CAMPAIGN_KEY, DATE_KEY
            HAVING COUNT(*) > 1
        )
    ''')[0]['CNT']
    if cnt == 0:
        _add(S5, '5.6', 'FACT_CAMPAIGN_DAILY: no duplicate campaign+date', 'PASS')
        print(f'  ✓ PASS  5.6     FACT_CAMPAIGN_DAILY: no duplicate campaign+date')
    else:
        _add(S5, '5.6', 'FACT_CAMPAIGN_DAILY: duplicate campaign+date', 'FAIL', f'{cnt:,} dupes')
        print(f'  ✗ FAIL  5.6     FACT_CAMPAIGN_DAILY: {cnt:,} duplicate campaign+date pairs')
except Exception as e:
    _add(S5, '5.6', 'FACT_CAMPAIGN_DAILY duplicates', 'ERROR', str(e)[:120])
    print(f'  ⚠ ERROR 5.6     Campaign dupes  →  {str(e)[:80]}')


# ════════════════════════════════════════════════════════════════════════════
# SECTION 6 — ROW COUNT HEALTH CHECK
# ════════════════════════════════════════════════════════════════════════════
S6 = '6. Row Counts'
print(f'\n{"="*70}')
print(f'  SECTION 6 — Row Count Health Check')
print(f'{"="*70}')

expected_counts = OrderedDict([
    ('DIM_DATE',                      (1000, None)),    # min, max (None = no upper limit)
    ('DIM_CHANNEL',                   (4,    4)),
    ('DIM_CUSTOMER',                  (500,  None)),
    ('DIM_PRODUCT',                   (50,   None)),
    ('DIM_CATEGORY',                  (4,    30)),
    ('DIM_MARKETING_CHANNEL',         (3,    10)),
    ('DIM_PAYMENT_METHOD',            (3,    15)),
    ('DIM_ORDER_STATUS',              (3,    10)),
    ('DIM_PAYMENT_STATUS',            (2,    10)),
    ('DIM_LOYALTY_TIER',              (3,    6)),
    ('DIM_PAYMENT_TERMS',             (2,    8)),
    ('DIM_CAMPAIGN',                  (5,    None)),
    ('FACT_ORDERS',                   (1000, None)),
    ('FACT_ORDERS_MOBILE_EXT',        (100,  None)),
    ('FACT_ORDERS_WHOLESALE_EXT',     (100,  None)),
    ('FACT_ORDERS_MARKETPLACE_EXT',   (100,  None)),
    ('FACT_ORDER_ITEMS',              (2000, None)),
    ('FACT_PAYMENTS',                 (1000, None)),
    ('FACT_SHIPMENTS',                (1000, None)),
    ('FACT_REVIEWS',                  (100,  None)),
    ('FACT_SUPPORT_TICKETS',          (50,   None)),
    ('FACT_USER_DAILY_ENGAGEMENT',    (100,  None)),
    ('FACT_CAMPAIGN_DAILY',           (50,   None)),
])

print(f'  {"Table":<40} {"Rows":>10}  {"Min":>7}  {"Max":>7}  Status')
print(f'  {"-"*40} {"-"*10}  {"-"*7}  {"-"*7}  {"-"*6}')

for table, (exp_min, exp_max) in expected_counts.items():
    try:
        cnt = _run(f'SELECT COUNT(*) AS CNT FROM {GOLD}.{table}')[0]['CNT']
        max_label = str(exp_max) if exp_max else '∞'
        if cnt == 0:
            _add(S6, table, f'{table} row count', 'FAIL', f'0 rows (expected ≥{exp_min})')
            print(f'  {table:<40} {cnt:>10,}  {exp_min:>7,}  {max_label:>7}  ✗ EMPTY')
        elif cnt < exp_min:
            _add(S6, table, f'{table} row count', 'WARN', f'{cnt:,} rows (expected ≥{exp_min})')
            print(f'  {table:<40} {cnt:>10,}  {exp_min:>7,}  {max_label:>7}  ⚠ LOW')
        elif exp_max and cnt > exp_max:
            _add(S6, table, f'{table} row count', 'WARN', f'{cnt:,} rows (expected ≤{exp_max})')
            print(f'  {table:<40} {cnt:>10,}  {exp_min:>7,}  {max_label:>7}  ⚠ HIGH')
        else:
            _add(S6, table, f'{table} row count', 'PASS', f'{cnt:,} rows')
            print(f'  {table:<40} {cnt:>10,}  {exp_min:>7,}  {max_label:>7}  ✓ OK')
    except Exception as e:
        _add(S6, table, f'{table} row count', 'ERROR', str(e)[:120])
        print(f'  {table:<40} {"N/A":>10}  {exp_min:>7,}  {"?":>7}  ⚠ ERROR')


# ════════════════════════════════════════════════════════════════════════════
# FINAL SUMMARY
# ════════════════════════════════════════════════════════════════════════════
total_tests = sum(_counters.values())
print(f'\n{"="*70}')
print(f'  GOLD DATA MODEL VALIDATION — FINAL SUMMARY')
print(f'{"="*70}')
print(f'  Total tests : {total_tests}')
print(f'  ✓ PASSED    : {_counters["pass"]}')
print(f'  ✗ FAILED    : {_counters["fail"]}')
print(f'  ⚠ WARNINGS  : {_counters["warn"]}')
print(f'  ⚠ ERRORS    : {_counters["error"]}')
print(f'{"="*70}')

if _counters['fail'] > 0:
    print(f'\n  ──── FAILED TESTS ────')
    for r in results:
        if r['status'] == 'FAIL':
            print(f'  ✗ [{r["id"]}] {r["name"]}')
            print(f'           {r["detail"]}')

if _counters['error'] > 0:
    print(f'\n  ──── ERRORS ────')
    for r in results:
        if r['status'] == 'ERROR':
            print(f'  ⚠ [{r["id"]}] {r["name"]}')
            print(f'           {r["detail"]}')

if _counters['warn'] > 0:
    print(f'\n  ──── WARNINGS ────')
    for r in results:
        if r['status'] == 'WARN':
            print(f'  ⚠ [{r["id"]}] {r["name"]}')
            print(f'           {r["detail"]}')

if _counters['fail'] == 0 and _counters['error'] == 0:
    print(f'\n  ✓ ALL TESTS PASSED — Gold data model is healthy.')
else:
    print(f'\n  ✗ {_counters["fail"]} failure(s) and {_counters["error"]} error(s) detected.')
    print(f'    Review the details above and fix the underlying load notebooks.')

print(f'{"="*70}')
