from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, avg, count, round, broadcast
import time

# --- CONFIG ---
MONGO_URI = "mongodb://bigdataenv-mongo-1:27017/tft_analytics"
TARGET_VERSION = "Releases/16"
MIN_GAMES = 50

# --- OPTIMIZED SPARK SESSION ---
spark = SparkSession.builder \
    .appName("TFT_Master_Processor") \
    .config("spark.mongodb.output.uri", MONGO_URI) \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "50") \
    .config("spark.default.parallelism", "50") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "512m") \
    .getOrCreate()

print(f"\n[INFO] Processing Data (Memory Optimized)...\n")

# 1. LOAD DATA
df = spark.read.json("hdfs://namenode:9000/tft/raw/*.json")
# df_filtered = df.filter(col("info.game_version").contains(TARGET_VERSION)) # Uncomment to filter
df_filtered = df

# 2. AGGRESSIVE PRUNING (Critical for Memory)
# Only select exactly what we need. Drop 'metadata', 'gameCreation', etc. immediately.
raw_participants = df_filtered.select(explode("info.participants").alias("p"))

# Flatten relevant columns NOW to free up memory from the nested JSON structure
participants = raw_participants.select(
    col("p.placement").alias("placement"),
    col("p.units").alias("units"),
    col("p.traits").alias("traits")
)
# Cache the SMALLER dataset
participants.cache()

print(f"DEBUG: Participants Cached. Count: {participants.count()}")

# ====================================================
# A. STAR LEVEL STATS & GLOBAL AVG
# ====================================================
print("--- Calculating Star Stats ---")
# Explode only units
units_exploded = participants.select("placement", explode("units").alias("u"))

# Calculate Global Averages (Unit + Star)
global_tier_stats = units_exploded.groupBy("u.character_id", "u.tier") \
    .agg(avg("placement").alias("global_avg")) \
    .withColumnRenamed("character_id", "unit_id") \
    .withColumnRenamed("tier", "tier_ref")

# Force calculation now to clear lineage
global_tier_stats.cache()
global_tier_stats.count()

# Calculate Play Rates
star_stats = units_exploded.groupBy("u.character_id", "u.tier") \
    .agg(avg("placement").alias("avg_place"), count("placement").alias("play_count")) \
    .filter(col("play_count") >= MIN_GAMES)

star_stats.write.format("mongo").mode("overwrite").option("collection", "unit_star_stats").save()

# ====================================================
# B. PARTNERS (Optimized Double Explode)
# ====================================================
print("--- Calculating Partner Deltas ---")
# 1. Explode Unit A (Keep 'units' array)
step1 = participants.select("placement", "units", explode("units").alias("uA"))

# 2. Select optimized columns for Unit A
step2 = step1.select(
    "placement",
    "units",
    col("uA.character_id").alias("unit_a"),
    col("uA.tier").alias("tier")
)

# 3. Explode Unit B
pairs = step2.select(
    "placement",
    "unit_a",
    "tier",
    explode("units").alias("uB")
).select(
    "placement",
    "unit_a",
    "tier",
    col("uB.character_id").alias("unit_b")
).filter(col("unit_a") != col("unit_b"))

# 4. Group
pair_stats = pairs.groupBy("unit_a", "tier", "unit_b") \
    .agg(avg("placement").alias("pair_avg"), count("placement").alias("count")) \
    .filter(col("count") >= MIN_GAMES)

# 5. Robust Join (No Broadcast to prevent Timeout)
unit_deltas = pair_stats.alias("ps").join(
        global_tier_stats.alias("gs"),
        (col("ps.unit_a") == col("gs.unit_id")) & (col("ps.tier") == col("gs.tier_ref"))
    ) \
    .withColumn("delta", round(col("ps.pair_avg") - col("gs.global_avg"), 2)) \
    .select(
        col("ps.unit_a").alias("unit_a"),
        col("ps.tier").alias("tier"),
        col("ps.unit_b").alias("unit_b"),
        col("delta"),
        col("ps.count")
    )

unit_deltas.write.format("mongo").mode("overwrite").option("collection", "explorer_units").save()

# ====================================================
# C. TRAITS & ITEMS (Standard)
# ====================================================
print("--- Calculating Trait & Item Deltas ---")

# ====================================================
# C. TRAITS (Unit + Tier + Trait)
# ====================================================
print("--- Calculating Trait Deltas (Tiered) ---")

# 1. Generate the base unit-trait stats
unit_trait = participants.select("placement", explode("units").alias("u"), "traits") \
    .select("placement", col("u.character_id").alias("unit_id"), col("u.tier").alias("tier"), explode("traits").alias("t")) \
    .filter(col("t.tier_current") > 0) \
    .select("placement", "unit_id", "tier", col("t.name").alias("trait"))

# 2. Group and Aggregate
trait_stats = unit_trait.groupBy("unit_id", "tier", "trait") \
    .agg(avg("placement").alias("trait_avg"), count("placement").alias("count")) \
    .filter(col("count") >= MIN_GAMES)

# 3. Join with Global Stats using explicit aliasing to avoid 'unit_id' ambiguity
trait_deltas = trait_stats.alias("ts").join(
    global_tier_stats.alias("gs"),
    (col("ts.unit_id") == col("gs.unit_id")) & (col("ts.tier") == col("gs.tier_ref"))
) \
    .withColumn("delta", round(col("ts.trait_avg") - col("gs.global_avg"), 2)) \
    .select(
        col("ts.unit_id"),
        col("ts.tier"),
        col("ts.trait"),
        col("delta"),
        col("ts.count")
    )

trait_deltas.write.format("mongo").mode("overwrite").option("collection", "explorer_traits").save()

# ====================================================
# D. ITEMS (Unit + Tier + Item)
# ====================================================
print("--- Calculating Item Deltas (Tiered) ---")

# 1. Generate the base unit-item stats
unit_items = units_exploded.select(
    "placement",
    col("u.character_id").alias("unit_id"),
    col("u.tier").alias("tier"),
    explode("u.itemNames").alias("item")
)

# 2. Group and Aggregate
item_stats = unit_items.groupBy("unit_id", "tier", "item") \
    .agg(avg("placement").alias("item_avg"), count("placement").alias("count")) \
    .filter(col("count") >= MIN_GAMES)

# 3. Join with Global Stats using explicit aliasing
item_deltas = item_stats.alias("is").join(
    global_tier_stats.alias("gs"),
    (col("is.unit_id") == col("gs.unit_id")) & (col("is.tier") == col("gs.tier_ref"))
) \
    .withColumn("delta", round(col("is.item_avg") - col("gs.global_avg"), 2)) \
    .select(
        col("is.unit_id"),
        col("is.tier"),
        col("is.item"),
        col("delta"),
        col("is.count")
    )

item_deltas.write.format("mongo").mode("overwrite").option("collection", "explorer_items").save()

print("--- SUCCESS ---")
spark.stop()