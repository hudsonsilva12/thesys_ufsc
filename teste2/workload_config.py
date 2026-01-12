# -*- coding: utf-8 -*-
# workload_config.py
# MySQL workload – M2Bench (schema REAL DEFINITIVO)

DEFAULT_RUNS_PER_TASK = 5
TASK_RUNS = {}

def resolve_database_name(sf: int) -> str:
    if sf == 1:
        return "ecommerce"
    return f"ecommerce_sf{sf}"

TASK_DEFINITIONS = [

    # --------------------------------------------------
    # Q1 – Scan simples de pedidos
    # --------------------------------------------------
    ("Q1_scan_orders", """
        SELECT
            o.order_id,
            o.customer_id,
            o.total_price
        FROM `Order` o;
    """),

    # --------------------------------------------------
    # Q2 – Contagem total de pedidos
    # --------------------------------------------------
    ("Q2_count_orders", """
        SELECT COUNT(*) AS total_orders
        FROM `Order`;
    """),

    # --------------------------------------------------
    # Q3 – Pedidos que contêm um produto existente
    # --------------------------------------------------
    ("Q3_orders_by_product", """
        SELECT DISTINCT o.order_id
        FROM `Order` o
        JOIN Order_line ol
            ON o.order_id = ol.order_id
        WHERE ol.product_id = (
            SELECT product_id
            FROM Order_line
            LIMIT 1
        );
    """),

    # --------------------------------------------------
    # Q4 – Detalhes completos de um pedido existente
    # --------------------------------------------------
    ("Q4_order_details", """
        SELECT
            o.order_id,
            o.customer_id,
            ol.product_id,
            ol.price
        FROM `Order` o
        JOIN Order_line ol
            ON o.order_id = ol.order_id
        WHERE o.order_id = (
            SELECT order_id
            FROM `Order`
            LIMIT 1
        );
    """),

    # --------------------------------------------------
    # Q5 – Pedidos sem itens caros (anti-join)
    # --------------------------------------------------
    ("Q5_orders_without_expensive_items", """
        SELECT o.order_id
        FROM `Order` o
        WHERE NOT EXISTS (
            SELECT 1
            FROM Order_line ol
            WHERE ol.order_id = o.order_id
              AND ol.price > 1000
        );
    """),

    # --------------------------------------------------
    # Q6 – Número de pedidos por cliente
    # --------------------------------------------------
    ("Q6_orders_per_customer", """
        SELECT
            o.customer_id,
            COUNT(*) AS total_orders
        FROM `Order` o
        GROUP BY o.customer_id;
    """),
]

