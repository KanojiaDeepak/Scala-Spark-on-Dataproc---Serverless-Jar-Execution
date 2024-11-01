import org.apache.spark.sql._
import org.apache.spark.sql.types._
import io.delta.tables._
import scala.sys.process._

//load tables of layer specified in argument into bigquery
object deltaToBigQuery{
    def main(args: Array[String]): Unit={

        val argMap=Args.toMap(args)

        val layer=Args.getValue("LAYER",argMap)
        val projectId=Args.getValue("PROJECT_ID",argMap)
        val delta_folder_path=Args.getValue("DELTA_FOLDER_PATH",argMap)
        val temp_bq_bucket=Args.getValue("TEMP_BQ_BUCKET",argMap)

        println(s"Moving $layer tables into bigquery")

        val gcsPath=delta_folder_path+"/"+layer

        val result=s"gcloud storage ls $gcsPath" .!!

        val foldersList=result.split("\n").map(_.trim).toList

        val spark:SparkSession = SparkSession.builder()
        .appName("loadToBigQuery")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()

        for (deltaPath <- foldersList){
            var df= DeltaLakeDAO.readData(spark,deltaPath)
            var tablePath=generateBigQueryPathFromDeltaPath(deltaPath,projectId)
            println(s"Writing data in $tablePath...")
            BigQueryDAO.writeData(spark,df,tablePath,temp_bq_bucket)
        }

        spark.stop()
    }

    def generateBigQueryPathFromDeltaPath(deltaPath: String,projectId: String): String ={
        var temp= deltaPath.split("/").map(_.trim).toList
        var dataset=temp(3)
        var table=temp(4)
        var tablePath=projectId+"."+dataset+"."+table
        return tablePath
    }
}