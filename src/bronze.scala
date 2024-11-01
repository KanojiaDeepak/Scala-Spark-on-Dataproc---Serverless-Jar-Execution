import org.apache.spark.sql._
import org.apache.spark.sql.types._
import io.delta.tables._

//load all tables excluding lookup tables
object bronze{
    def main(args: Array[String]): Unit={
        val argMap=Args.toMap(args)

        val delta_bronze_layer_path=Args.getValue("DELTA_FOLDER_PATH",argMap)+"/bronze/"
        val raw_folder_path=Args.getValue("RAW_FOLDER_PATH",argMap)
        
        println("Creating bronze layer...")

        val raw_address=s"$raw_folder_path/address.csv"
        val raw_customer=s"$raw_folder_path/customer.csv"
        val raw_order=s"$raw_folder_path/order.csv"
        val raw_order_item=s"$raw_folder_path/order_item.csv"
        val raw_payment=s"$raw_folder_path/payment.csv"
        val raw_payment_method=s"$raw_folder_path/payment_method.csv"
        val raw_product=s"$raw_folder_path/product.csv"

        val spark:SparkSession = SparkSession.builder()
        .appName("bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()

        val address=CsvDAO.readData(spark, raw_address)
        DeltaLakeDAO.writeData(spark,address,delta_bronze_layer_path+"address/")

        val customer=CsvDAO.readData(spark, raw_customer)
        DeltaLakeDAO.writeData(spark,customer,delta_bronze_layer_path+"customer/")

        val order=CsvDAO.readData(spark, raw_order)
        DeltaLakeDAO.writeData(spark,order,delta_bronze_layer_path+"order/")

        val order_item=CsvDAO.readData(spark, raw_order_item)
        DeltaLakeDAO.writeData(spark,order_item,delta_bronze_layer_path+"order_item/")

        val payment=CsvDAO.readData(spark, raw_payment)
        DeltaLakeDAO.writeData(spark,payment,delta_bronze_layer_path+"payment/")

        val payment_method=CsvDAO.readData(spark, raw_payment_method)
        DeltaLakeDAO.writeData(spark,payment_method,delta_bronze_layer_path+"payment_method/")

        val product=CsvDAO.readData(spark, raw_product)
        DeltaLakeDAO.writeData(spark,product,delta_bronze_layer_path+"product/")

        spark.stop()
    }
}