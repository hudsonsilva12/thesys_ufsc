# -*- coding: utf-8 -*-
# workload_config_mongo.py
# MongoDB workload – M2Bench
# Schema REAL confirmado via db.orders.findOne()

MONGO_URI = "mongodb://admin:admin@localhost:27018/?authSource=admin"

MONGO_DB_BY_SF = {
    1: "ecommerce",
    10: "m2bench_sf10",
    30: "m2bench_sf30",
    100: "m2bench_sf100",
}

DEFAULT_RUNS_PER_TASK = 5
TASK_RUNS = {}

TASK_DEFINITIONS = {

    # --------------------------------------------------
    # Q1 – Scan simples de pedidos
    # --------------------------------------------------
    "Q1_scan_orders": {
        "collection": "orders",
        "pipeline": [
            {
                "$project": {
                    "_id": 0,
                    "order_id": 1,
                    "customer_id": 1,
                    "total_price": 1
                }
            }
        ],
    },

    # --------------------------------------------------
    # Q2 – Contagem total de pedidos
    # --------------------------------------------------
    "Q2_count_orders": {
        "collection": "orders",
        "pipeline": [
            { "$count": "total_orders" }
        ],
    },

    # --------------------------------------------------
    # Q3 – Pedidos que contêm um produto específico
    # --------------------------------------------------
    "Q3_orders_by_product": {
        "collection": "orders",
        "pipeline": [
            { "$match": { "order_line.product_id": 4507 } },
            { "$project": { "_id": 0, "order_id": 1 } }
        ],
    },

    # --------------------------------------------------
    # Q4 – Detalhes completos de um pedido
    # --------------------------------------------------
    "Q4_order_details": {
        "collection": "orders",
        "pipeline": [
            { "$match": { "order_id": "O1" } },
            { "$unwind": "$order_line" },
            {
                "$project": {
                    "_id": 0,
                    "order_id": 1,
                    "customer_id": 1,
                    "product_id": "$order_line.product_id",
                    "unit_price": "$order_line.unit_price",
                    "quantity": "$order_line.quantity",
                    "subtotal": "$order_line.subtotal"
                }
            }
        ],
    },

    # --------------------------------------------------
    # Q5 – Pedidos sem itens caros
    # --------------------------------------------------
    "Q5_orders_without_expensive_items": {
        "collection": "orders",
        "pipeline": [
            {
                "$match": {
                    "order_line": {
                        "$not": {
                            "$elemMatch": {
                                "unit_price": { "$gt": 1000 }
                            }
                        }
                    }
                }
            },
            { "$project": { "_id": 0, "order_id": 1 } }
        ],
    },

    # --------------------------------------------------
    # Q6 – Número de pedidos por cliente
    # --------------------------------------------------
    "Q6_orders_per_customer": {
        "collection": "orders",
        "pipeline": [
            {
                "$group": {
                    "_id": "$customer_id",
                    "total_orders": { "$sum": 1 }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "customer_id": "$_id",
                    "total_orders": 1
                }
            }
        ],
    },
}

