import org.apache.spark.sql._
import org.apache.spark.sql.types._
import io.delta.tables._

//load lookup tables
object setup{
    def main(args: Array[String]): Unit={
        
        val argMap=Args.toMap(args)

        val delta_lookup_path=Args.getValue("DELTA_FOLDER_PATH",argMap)+"/setup/"
        val raw_folder_path=Args.getValue("RAW_FOLDER_PATH",argMap)

        val raw_order_item_status_lookup=s"$raw_folder_path/order_item_status_lookup.csv"
        val raw_order_status_lookup=s"$raw_folder_path/order_status_lookup.csv"
        
        println("Running setup...")

        val spark:SparkSession = SparkSession.builder()
        .appName("setup")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()

        val order_item_status_lookup=CsvDAO.readData(spark, raw_order_item_status_lookup)
        DeltaLakeDAO.writeData(spark,order_item_status_lookup,delta_lookup_path+"order_item_status_lookup/")

        val order_status_lookup=CsvDAO.readData(spark, raw_order_status_lookup)
        DeltaLakeDAO.writeData(spark,order_status_lookup,delta_lookup_path+"order_status_lookup/")

        spark.stop()
    }
}