package org.apache.spark.sql.test

import com.databricks.labs.mosaic._
import com.databricks.labs.mosaic.gdal.MosaicGDAL
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.gdal.gdal.gdal

import java.nio.file.Files
import scala.util.Try

trait SharedSparkSessionGDAL extends SharedSparkSession {

    override def sparkConf: SparkConf = {
        super.sparkConf
            .set(MOSAIC_GDAL_NATIVE, "true")
    }

    override def createSparkSession: TestSparkSession = {
        val conf = sparkConf
        conf.set(MOSAIC_RASTER_CHECKPOINT, Files.createTempDirectory("mosaic").toFile.getAbsolutePath)
        SparkSession.cleanupAnyExistingSession()
        val session = new TestSparkSession(conf)
        session.sparkContext.setLogLevel("FATAL")
        Try {
            MosaicGDAL.enableGDAL(session)
        }
        session
    }

    override def beforeEach(): Unit = {
        super.beforeEach()
        MosaicGDAL.enableGDAL(this.spark)
        gdal.AllRegister()
    }
    
}
