import org.apache.spark.sql.SparkSession;

public class SparkIngestMain {

    public static void main(String[] args) {
        final SparkSession spark = SparkSession.builder()
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .appName("Java Spark SQL basic example")
                .getOrCreate();

        spark.sql("CREATE TABLE IF NOT EXISTS delta.`/tmp/delta-table` USING DELTA AS SELECT col1 as id FROM VALUES 0,1,2,3,4");
        var df = spark.sql("SELECT * FROM delta.`/tmp/delta-table`");
    }
}
