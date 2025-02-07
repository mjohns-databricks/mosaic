==================
Installation guide
==================

Supported platforms
###################

.. note::
    For Mosaic 0.4 series, we recommend DBR 13.3 LTS on Photon or ML Runtime clusters.

Mosaic 0.4.x series only supports DBR 13.x DBRs. If running on a different DBR it will throw an exception:

**DEPRECATION ERROR: Mosaic v0.4.x series only supports Databricks Runtime 13. You can specify
`%pip install 'databricks-mosaic<0.4,>=0.3'` for DBR < 13.**

Mosaic 0.4.x series issues an ERROR on standard, non-Photon clusters `ADB <https://learn.microsoft.com/en-us/azure/databricks/runtime/>`_ |
`AWS <https://docs.databricks.com/runtime/index.html/>`_ |
`GCP <https://docs.gcp.databricks.com/runtime/index.html/>`_ :

**DEPRECATION ERROR: Please use a Databricks Photon-enabled Runtime for performance benefits or Runtime ML for spatial
AI benefits; Mosaic 0.4.x series restricts executing this cluster.**

As of Mosaic 0.4.0 (subject to change in follow-on releases)

   * `Assigned Clusters <https://docs.databricks.com/en/compute/configure.html#access-modes>`_ : Mosaic Python, SQL, R, and Scala APIs.
   * `Shared Access Clusters <https://docs.databricks.com/en/compute/configure.html#access-modes>`_ : Mosaic Scala API (JVM) with
     Admin `allowlisting <https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/allowlist.html>`_ ;
     Python bindings to Mosaic Scala APIs are blocked by Py4J Security on Shared Access Clusters.

.. note::
   As of Mosaic 0.4.0 (subject to change in follow-on releases)

   * `Unity Catalog <https://www.databricks.com/product/unity-catalog>`_ : Enforces process isolation which is difficult to
      accomplish with custom JVM libraries; as such only built-in (aka platform provided) JVM APIs can be invoked from other
      supported languages in Shared Access Clusters.
   * `Volumes <https://docs.databricks.com/en/connect/unity-catalog/volumes.html>`_ : Along the same principle of isolation,
      clusters (both assigned and shared access) can read Volumes via relevant built-in readers and writers or via custom
      python calls which do not involve any custom JVM code.

If you have cluster creation permissions in your Databricks
workspace, you can create a cluster using the instructions
`here <https://docs.databricks.com/clusters/create.html#use-the-cluster-ui>`_.

You will also need "Can Manage" permissions on this cluster in order to attach the
Mosaic library to your cluster. A workspace administrator will be able to grant 
these permissions and more information about cluster permissions can be found 
in our documentation
`here <https://docs.databricks.com/security/access-control/cluster-acl.html#cluster-level-permissions>`_.

Package installation
####################

Installation from PyPI
**********************
Python users can install the library directly from `PyPI <https://pypi.org/project/databricks-mosaic/>`_
using the instructions `here <https://docs.databricks.com/libraries/cluster-libraries.html>`_
or from within a Databricks notebook using the :code:`%pip` magic command, e.g.

.. code-block:: bash

    %pip install databricks-mosaic

When you install with :code:`%pip`, the JAR is availalbe from the Python WHL (see the "Enabling" section for more); also,
if you need to install Mosaic 0.3 series for DBR 12.2 LTS, e.g.

.. code-block:: bash

    %pip install "databricks-mosaic<0.4,>=0.3"

For Mosaic versions < 0.4 please use the `0.3 docs <https://databrickslabs.github.io/mosaic/v0.3.x/index.html>`_.

Installation from release artifacts
***********************************
Alternatively, you can access the latest release artifacts `here <https://github.com/databrickslabs/mosaic/releases>`_
and manually attach the appropriate library to your cluster.

Which artifact you choose to attach will depend on the language API you intend to use.

* For Python API users, choose the Python .whl file (includes the JAR)
* For Scala users, take the Scala JAR (packaged with all necessary dependencies).
* For R users, download the Scala JAR and the R bindings library [see the sparkR readme](R/sparkR-mosaic/README.md).

Instructions for how to attach libraries to a Databricks cluster can be found `here <https://docs.databricks.com/libraries/cluster-libraries.html>`_.

Automated SQL registration
**************************
If you would like to use Mosaic's functions in pure SQL (in a SQL notebook, from a business intelligence tool,
or via a middleware layer such as Geoserver, perhaps) then you can configure
"Automatic SQL Registration" using the instructions `here <https://databrickslabs.github.io/mosaic/usage/automatic-sql-registration.html>`_.

Enabling the Mosaic functions
#############################
The mechanism for enabling the Mosaic functions varies by language:

.. tabs::
   .. code-tab:: py

    import mosaic as mos
    mos.enable_mosaic(spark, dbutils)

   .. code-tab:: scala

    import com.databricks.labs.mosaic.functions.MosaicContext
    import com.databricks.labs.mosaic.H3
    import com.databricks.labs.mosaic.JTS

    val mosaicContext = MosaicContext.build(H3, JTS)
    import mosaicContext.functions._

   .. code-tab:: r R

    library(sparkrMosaic)
    enableMosaic()

.. note::
    We recommend use of `import mosaic as mos` to namespace the python api and avoid any conflicts with other similar
    functions. By default, the python import will handle installing the JAR and registering Hive UDFs which is suitable
    for Assigned (vs Shared Access) clusters.

Unless you are specially adding the JAR to your cluster (outside :code:`%pip` or the WHL file), please always initialize
with Python first, then you can initialize Scala (after the JAR has been auto-attached by python); otherwise, you don't
need to initialize Scala unless you are using that language binding. You can further configure Mosaic enable with spark
confs as well as through extra params in Mosaic 0.4.x series :code:`enable_mosaic` function.

.. function:: enable_mosaic()

    Use this function at the start of your workflow to ensure all the required dependencies are installed and
    Mosaic is configured according to your needs.

    :param spark: The active spark session.
    :type spark: pyspark.sql.SparkSession
    :param dbutils: Specify dbutils object used for :code:`display` and :code:`displayHTML` functions, needed for Kepler integration (Optional, default is None).
    :type dbutils: dbruntime.dbutils.DBUtils
    :param log_info: True will try to setLogLevel to "info", False will not (Optional, default is False).
    :type log_info: bool
    :param jar_path: If provided, sets :code:`"spark.databricks.labs.mosaic.jar.path"` (Optional, default is None).
    :type jar_path: str
    :param jar_autoattach: False will not registers the JAR; sets :code:`"spark.databricks.labs.mosaic.jar.autoattach"` to False, True will register the JAR (Optional, default is True).
    :type jar_autoattach: bool
    :rtype: None

Users can control various aspects of Mosaic's operation with the following Spark confs:

    * :code:`"spark.databricks.labs.mosaic.jar.autoattach"` - Automatically attach the Mosaic JAR to the Databricks cluster (Optional, default is "true").
    * :code:`"spark.databricks.labs.mosaic.jar.path"` - Explicitly specify the path to the Mosaic JAR (Optional and not required at all in a standard Databricks environment).
    * :code:`"spark.databricks.labs.mosaic.geometry.api"` - Explicitly specify the underlying geometry library to use for spatial operations (Optional, default is "JTS").
    * :code:`"spark.databricks.labs.mosaic.index.system"` - Explicitly specify the index system to use for optimized spatial joins (Optional, default is "H3").


SQL usage
*********
If you have not employed :ref:`Automatic SQL registration` (on by default and handled by Python enable in notebook), you will need to
register the Mosaic SQL functions in your SparkSession from a Scala notebook cell:

.. code-block:: scala

    import com.databricks.labs.mosaic.functions.MosaicContext
    import com.databricks.labs.mosaic.H3
    import com.databricks.labs.mosaic.JTS

    val mosaicContext = MosaicContext.build(H3, JTS)
    mosaicContext.register(spark)

.. warning::
    Mosaic 0.4.x SQL bindings for DBR 13 can register with Assigned clusters (as Hive UDFs), but not Shared Access due
    to `Unity Catalog <https://www.databricks.com/product/unity-catalog>`_ API changes, more `here <https://docs.databricks.com/en/udf/index.html>`_.
