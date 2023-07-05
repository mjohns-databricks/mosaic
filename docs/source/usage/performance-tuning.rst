=======================
Performance Tuning
=======================

Data Engineering
################

It is important for customers to have Data Engineering (DE) tables tuned for performance. Mosaic has been designed to operate directly on standard interchange formats which may appear in various DataFrame columns; currently, WKT, WKB, and GeoJSON are supported, with WKB offering the best properties for storing and querying. Further, while capabilities are always advancing, Mosaic does not expose geometry types as defined in `OGC Simple Features for SQL <https://www.ogc.org/standard/sfs/>`__, making investments in DE all the more beneficial. Additionally, for best tuning, pre-standardize all your geospatial data into the same SRID; for most applications, this should be 4326, see `CRS Docs <https://databrickslabs.github.io/mosaic/usage/grid-indexes-bng.html#coordinate-reference-system>`__. *As of Mosaic 0.3 series, customers need to prepare tables for performant queries, though we are exploring options to improve this experience going forward.*

Polygons & Lines
****************  
The best pattern for DE looks something like the following (column names are notional):

1. Call Mosaic's `grid_tessellateexplode <https://databrickslabs.github.io/mosaic/api/spatial-indexing.html#grid-tessellateexplode>`__ on your geometries at a given resolution; in addition to exploding to per row, this adds an ``index`` column, a struct with the following fields: ``is_core``, ``index_id``, ``wkb``; you essentially now have the result of H3 covers operation on the geometries and knowledge of which index space is fully contained within the geometry and WKB chips for the boundaries as well as core index; this is the fundamental call for performance
2. Call something like ``select index.*, * except(index)`` to pull the struct fields into top level columns; also, adding them first to ensure they are part of the table statistics; you can be more thoughtful based on your data, e.g. don't need WKB in the statistics
3. Save the table as delta lake and call ``optimize <table or delta path> zorder (index_id)`` to get proper layout

Points
******
1. Points can be easily placed into a resolution as a point will only exists in a single cell; one strategy can be to populate a field at resolution 15 (the lowest for H3) and then use `Databricks Photon APIs`_ to efficiently get to any higher parent
2. Save the table as delta lake and call ``optimize <table or delta path> zorder (index_id)`` to get proper layout  
                                                                                                                                                                                      
Optimizing Queries
##################                                                                                                                                                                                    
                                                                                                                                                                                    
Once DE steps have been accomplished, your query pattern can look like the following, e.g. for point-in-polygon:

(a) when you are only interested in approximate results, compare only for cell matches

.. code-block:: sql
                                                                                                                                                                                    
    -- H3 APPROXIMATE ONLY RESULTS
    SELECT *
    FROM poly_tbl as L, pnt_tbl as R
    WHERE L.index_id = R.index_id

(b) when you are interested in precise results, test boundary chip information

.. code-block:: sql
                                                                                                                                                                                    
    -- H3 + PRECISE RESULTS
    SELECT *
    FROM poly_tbl as L, pnt_tbl as R
    WHERE 
        L.index_id = R.index_id AND
        (L.is_core OR st_contains(L.wkb, R.geom_wkb))

*If only H3 approximate results are needed, you have the option to drop the WKB in tables after performing DE steps.*

These are the main steps; however, when indexing wide area polygons as found in worldwide data, we would recommend a little more nuance to the core pattern, e.g. considering there are `1.6 Trillion cells <https://h3geo.org/docs/core-library/restable/>`__ that tessellate the globe at H3 resolution 12,  but only 4.8 billion at 9 (default ``res_b`` below) and 2 million at 5 (default ``res_a`` below).  For large areas, recommend a pattern like the following (showing python):

.. code-block:: py
                                                                                                                                                                                      
    df_a = (
      spark.table(tbl_name)
        .repartition(num_parts) # (optional)
        .selectExpr(
          f"* except({in_geom_col})",
          f"grid_tessellateexplode({in_geom_col}, {res_a})"
        )
        .selectExpr(
          f"{res_a} as {res_col}",
          "index.*",
          "* except(index)"
        )
        .withColumnRenamed("index_id", cell_col)
        .withColumnRenamed("wkb", out_geom_col)
        .withColumnRenamed("is_core", is_core_col)
    )

    df_b = (
      df_a
        .filter(f"{is_core_col} = false")
        .selectExpr(
          f"* except({cell_col}, {is_core_col}, {out_geom_col}, {res_col})",
          f"grid_tessellateexplode({out_geom_col}, {res_b})"
        )
        .selectExpr(
          f"{res_b} as {res_col}",
          "index.*",
          "* except(index)"
        )
        .withColumnRenamed("index_id", cell_col)
        .withColumnRenamed("wkb", out_geom_col)
        .withColumnRenamed("is_core", is_core_col)
    )

    df_res = (
      df_a.filter(f"{is_core_col} = true")
        .union(df_b)
    )

    ## - While going above 9 is "doable", e.g.
    ##   11 bloats the number of rows by multiple of 7^2 = 49
    ##   13 bloats the number of rows by multiple of 7^4 = 2,401

    sql(f"""DROP TABLE IF EXISTS {tbl_fqn}""")

    (
      df_res
        .write
          .option("mode", "overwrite")
        .saveAsTable(tbl_fqn)
    )  

    sql(f"""OPTIMIZE {tbl_fqn} ZORDER BY ({cell_col})""")

This pattern allows us to level out polygon "worst case" max areas, e.g. resolution 5 and then down to 9 for large area boundaries.  You will notice that only ``is_core`` is checked at resolution 5 which is cheap, then resolution 9 has both ``is_core`` and ``st_contains`` in the clause. That query pattern then looks like (showing SQL):

.. code-block:: sql

    -- RESULTS WITH H3 INFORMATION
    SELECT *
    FROM poly_tbl as L, pnt_tbl as R
    WHERE 
        (L.h3_res = 5 AND h3_toparent(R.h3_15, 5) = L.h3_cell)   -- CORE RES=5 MATCH?
        OR (
            (L.h3_res = 9 AND h3_toparent(R.h3_15, 9) = L.h3_cell) -- RES=9 MATCH?
        AND(
            L.h3_is_core OR st_contains(L.h3_wkb, R.geom_wkb)))

Adjust the query as follows for results that strip back the H3 information (more akin to traditional ``ST_`` functions):

.. code-block:: sql

    -- RESULTS WITHOUT H3 INFORMATION @ GEOMETRY-CENTRIC
    SELECT DISTINCT * except(L.h3_res, L.h3_is_core, L.h3_cell, L.h3_wkb, R.h3_15, R.geom_wkb)
    FROM poly_tbl as L, pnt_tbl as R
    WHERE 
        (L.h3_res = 5 AND h3_toparent(R.h3_15, 5) = L.h3_cell)   -- CORE RES=5 MATCH?
        OR (
            (L.h3_res = 9 AND h3_toparent(R.h3_15, 9) = L.h3_cell) -- RES=9 MATCH?
        AND(
            L.h3_is_core OR st_contains(L.h3_wkb, R.geom_wkb)))
                                                                                                                                                                                                             
Databricks Photon APIs
######################                                                                                                                                                                                                             

Since DBR 11.2, Databricks `Photon Runtimes <https://docs.databricks.com/runtime/photon.html>`__ have delivered `H3 APIs <https://docs.databricks.com/sql/language-manual/sql-ref-h3-geospatial-functions.html>`__ that are blazing fast. Mosaic leverages these APIs everywhere possible in its implementation. They are pre-loaded in SQL, meaning Spark SQL on DBR as well as DBSQL. 

For python: 

.. code-block:: py

    from pyspark.databricks.sql.functions import *

For scala: 

.. code-block:: scala

    import com.databricks.sql.functions._

Tuning Tips
###########                                             

Spark `Adaptive Query Exection (AQE) <https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution>`__ is tuned for data-heavy processing where each row has a cost. It likes those rows to be even and has a hard time reasoning about hidden compute costs as incurred when ``ST_`` functions, e.g. ``ST_Contains`` are invoked. To bring more control AQE can be turned completely off:

.. code-block:: py

    spark.conf.set("spark.databricks.optimizer.adaptive.enabled", False) # <- default is True
    spark.conf.set("spark.sql.adaptive.enabled", False)                  # <- default is True

AQE can also be left on with other controls more finely managing its behavior, such as:

.. code-block:: py

    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)                  # <- default is True, may want False
    spark.conf.set("spark.sql.adaptive.coalescePartitions.parallelismFirst", False)        # <- default is True (respect size)
    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "24MB")              # <- default is 64MB
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", 2)                 # <- default is 5
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "64MB")  # <- default is 256MB

Shuffle partition control is also useful for larger data:

.. code-block:: py
  
    spark.conf.set("spark.sql.shuffle.partitions", 256)                              # <- default is 200
    spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", 256) # <- default is shuffle setting (200)

Broadcast thresholds can be useful to manage, e.g. when doing compute heavy operations (vs approximate), you may want to ensure data is partitioned instead of broadcasted:

.. code-block:: py
  
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", '10485760b')          # <- default is '10485760b' or 10m ('2147483648b' for 2gb | '0b')
    spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", '10485760b') # <- default is ''

Adjusting settings for Delta file sizes when also using AQE can be beneficial; however, these are becoming increasingly "smartly" auto-managed:

.. code-block:: py

    spark.conf.set("spark.databricks.delta.properties.defaults.targetFileSize", '50331648')     # <- default is not set ('16777216' for 16MB | '50331648' or 48MB) vs ~128MB
    spark.conf.set("spark.databricks.delta.properties.defaults.tuneFileSizesForRewrites", True) # <- default is not set 

There are various partitioning and join hints that may also add advantages when tuning [`Databricks Docs <https://docs.databricks.com/sql/language-manual/sql-ref-syntax-qry-select-hints.html>`__ | `Spark Docs <https://spark.apache.org/docs/latest/sql-performance-tuning.html#join-strategy-hints-for-sql-queries>`__].
