import org.apache.spark.sql._
import org.apache.spark.sql.types._
import io.delta.tables._

//creates silver tables
object silver{
    def main(args: Array[String]): Unit={
        val argMap=Args.toMap(args)

        val delta_folder_path=Args.getValue("DELTA_FOLDER_PATH",argMap)
        val delta_silver_layer_path=delta_folder_path+"/silver/"
        
        println("Creating silver layer...")
        
        val spark:SparkSession = SparkSession.builder()
        .appName("silver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()

        val order_item=DeltaLakeDAO.readData(spark,s"$delta_folder_path/bronze/order_item/")
        order_item.createOrReplaceTempView("order_item")

        val order=DeltaLakeDAO.readData(spark,s"$delta_folder_path/bronze/order/")
        order.createOrReplaceTempView("order")

        val order_item_status_lookup=DeltaLakeDAO.readData(spark,s"$delta_folder_path/setup/order_item_status_lookup/")
        order_item_status_lookup.createOrReplaceTempView("order_item_status_lookup")

        val order_status_lookup=DeltaLakeDAO.readData(spark,s"$delta_folder_path/setup/order_status_lookup/")
        order_status_lookup.createOrReplaceTempView("order_status_lookup")
        
        val product=DeltaLakeDAO.readData(spark,s"$delta_folder_path/bronze/product/")
        product.createOrReplaceTempView("product")
        
        val order_item_silver=spark.sql("""
        SELECT
            oi.order_id,
            o.order_status,
            o.order_amount,
            o.currency,
            oi.order_item_id,
            oisl.order_item_status as order_item_status,
            oi.product_id,
            p.product_name,
            oi.quantity,
            date_format(o.order_date, 'yyyy-MM-dd') as order_date,
            o.order_date as order_utc_date,
            year(o.order_date) as order_year,
            month(o.order_date) as order_month,
            weekofyear(o.order_date) as order_week
        FROM
            order_item oi
        LEFT JOIN (
            SELECT
                o.*,
                osl.order_status
            FROM
                order o
            LEFT JOIN order_status_lookup osl ON o.status = osl.order_status_lookup_id
        ) o ON oi.order_id = o.order_id
        LEFT JOIN product p ON oi.product_id = p.product_id
        LEFT JOIN order_item_status_lookup oisl ON oi.status = oisl.order_item_status_lookup_id
        ORDER BY
            o.order_id,
            oi.order_item_id
        """)

        DeltaLakeDAO.writeData(spark,order_item_silver,delta_silver_layer_path+"order_item_silver/")

        spark.stop()
    }
}