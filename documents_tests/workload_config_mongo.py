# ============================================================
# MongoDB Connection
# ============================================================

MONGO_URI = "mongodb://admin:admin@localhost:27018/?authSource=admin"


# ============================================================
# MongoDB Databases por Scale Factor (SF)
# ============================================================

MONGO_DB_BY_SF = {
    1: "ecommerce",
    10: "m2bench_sf10",
    30: "m2bench_sf30",
    100: "m2bench_sf100",
}


# ============================================================
# Configuração de Execução
# ============================================================

DEFAULT_RUNS_PER_TASK = 5

TASK_RUNS = {
    # pode sobrescrever runs por task se quiser
    # "M1_TR1_denormalized_scan": 5,
}


# ============================================================
# Tasks – Regras ER + Query
# ============================================================

TASK_DEFINITIONS = {

    # --------------------------------------------------------
    # T-R1 — Denormalized + scans
    # --------------------------------------------------------
    "M1_TR1_denormalized_scan": {
        "collection": "orders",
        "pipeline": [
            {"$group": {
                "_id": None,
                "total_revenue": {"$sum": "$total_price"},
                "num_orders": {"$sum": 1}
            }},
            {"$project": {
                "_id": 0,
                "total_revenue": 1,
                "num_orders": 1
            }}
        ],
    },

    # --------------------------------------------------------
    # T-R2 — Document-centric CRUD (read)
    # --------------------------------------------------------
    "M2_TR2_single_order_lookup": {
        "collection": "orders",
        "pipeline": [
            {"$match": {"order_id": {"$exists": True}}},
            {"$limit": 1}
        ],
    },

    # --------------------------------------------------------
    # T-R3 — Normalized logic simulated (heavy unwind)
    # --------------------------------------------------------
    "M3_TR3_join_like_unwind": {
        "collection": "orders",
        "pipeline": [
            {"$unwind": "$order_line"},
            {"$group": {
                "_id": "$order_line.product_id",
                "total_revenue": {"$sum": "$order_line.subtotal"}
            }},
            {"$project": {
                "_id": 0,
                "product_id": "$_id",
                "total_revenue": 1
            }},
            {"$sort": {"total_revenue": -1}}
        ],
    },

    # --------------------------------------------------------
    # T-R4 — Indexed attribute filtering
    # --------------------------------------------------------
    "M4_TR4_filter_by_product": {
        "collection": "orders",
        "pipeline": [
            {"$unwind": "$order_line"},
            {"$match": {"order_line.product_id": {"$exists": True}}},
            {"$limit": 100}
        ],
    },
}

