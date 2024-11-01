import org.apache.spark.sql._
import org.apache.spark.sql.types._
import io.delta.tables._

//creates gold tables
object gold{
    def main(args: Array[String]): Unit={

        val argMap=Args.toMap(args)

        val delta_folder_path=Args.getValue("DELTA_FOLDER_PATH",argMap)
        val delta_gold_layer_path=delta_folder_path+"/gold/"
        
        println("Creating gold layer...")

        val spark:SparkSession = SparkSession.builder()
        .appName("gold")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()

        val order_item_silver=DeltaLakeDAO.readData(spark,s"$delta_folder_path/silver/order_item_silver/")
        order_item_silver.createOrReplaceTempView("order_item_silver")

        val order=DeltaLakeDAO.readData(spark,s"$delta_folder_path/bronze/order/")
        order.createOrReplaceTempView("order")

        //Calculate order by date
        val orderByDate=spark.sql("""
        SELECT
            date_format(order_date, 'yyyy-MM-dd') as order_date,
            count(*) as order_count
        FROM
            order
        WHERE
            status != 2
        GROUP BY
            1
        ORDER BY
            1 DESC
        """)

        DeltaLakeDAO.writeData(spark,orderByDate,delta_gold_layer_path+"orderByDate/")
        
        //Calculate order total by date
        val orderTotalByDate=spark.sql("""
        SELECT
            date_format(order_date, 'yyyy-MM-dd') as order_date,
            sum(order_amount) as order_total
        FROM
            order
        WHERE
            status != 2
        GROUP BY
            1
        ORDER BY
            1 DESC
        """)

        DeltaLakeDAO.writeData(spark,orderTotalByDate,delta_gold_layer_path+"orderTotalByDate/")


        //Total orders by product per week
        val ordersByProductPerWeek=spark.sql("""
        SELECT
            product_id,
            product_name,
            order_week,
            sum(quantity) as total_quantity
        FROM
            order_item_silver
        WHERE
            order_status != 'Canceled'
        GROUP BY
            1,
            2,
            3
        ORDER BY
            1,
            3
        """)

        DeltaLakeDAO.writeData(spark,ordersByProductPerWeek,delta_gold_layer_path+"ordersByProductPerWeek/")

        
        //Most sold product in last month
        val mostSoldProductLastMonth=spark.sql("""
        SELECT
            product_id,
            product_name,
            sum(quantity) as total_quantity
        FROM
            order_item_silver
        WHERE
            order_status != 'Canceled'
            AND order_month = month(add_months(CURRENT_DATE, -1))
        GROUP BY
            product_id,
            product_name
        ORDER BY
            total_quantity desc
        LIMIT
            10
        """)

        DeltaLakeDAO.writeData(spark,mostSoldProductLastMonth,delta_gold_layer_path+"mostSoldProductLastMonth/")

        spark.stop()
    }
}