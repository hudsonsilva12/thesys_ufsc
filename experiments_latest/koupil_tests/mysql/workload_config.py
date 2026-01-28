# -*- coding: utf-8 -*-

# ================================
# CONFIGURAÇÃO GERAL
# ================================

DEFAULT_RUNS_PER_TASK = 5
TASK_RUNS = {}

# ================================
# RESOLUÇÃO DO NOME DO DATABASE
# ================================

def resolve_database_name(sf: int) -> str:
    """
    Regra:
    SF = 1   -> ecommerce
    SF > 1   -> ecommerce_sf1<SF>
    """
    if sf == 1:
        return "ecommerce_sf"+str(sf)
    return f"ecommerce_sf{sf}"

# ================================
# T-R1 – Desnormalizado + Scans
# ================================
SQL_TR1_ORDER_SCAN = """
SELECT SUM(o.total_price) AS total_revenue,
COUNT(*) AS num_orderes FROM `Order` o WHERE o.total_price > 0;
"""

# ================================
# T-R2 – Documento-centric CRUD
# ================================
SQL_TR2_SINGLE_ORDER = """
SELECT
    o.order_id,
    o.customer_id,
    o.total_price
FROM `Order` o
WHERE o.order_id = (
    SELECT order_id FROM `Order` LIMIT 1
);
"""

# ================================
# T-R3 – Normalizado + Joins
# ================================
SQL_TR3_PRODUCT_REVENUE = """
SELECT
    product_id,
    SUM(price) AS total_revenue
FROM Order_line
GROUP BY product_id
ORDER BY total_revenue DESC;
"""

# ================================
# T-R4 – Normalizado + Índices
# ================================
SQL_TR4_PRODUCT_FILTER = """
SELECT DISTINCT
    o.order_id
FROM Order_line ol
JOIN `Order` o ON o.order_id = ol.order_id
WHERE ol.product_id = (
    SELECT product_id FROM Product LIMIT 1
);
"""

# ================================
# DEFINIÇÃO DAS TASKS
# ================================
TASK_DEFINITIONS = [
    ("T-R1_denorm_scan", SQL_TR1_ORDER_SCAN),
    ("T-R2_single_order", SQL_TR2_SINGLE_ORDER),
    ("T-R3_join_revenue", SQL_TR3_PRODUCT_REVENUE),
    ("T-R4_index_filter", SQL_TR4_PRODUCT_FILTER),
]

