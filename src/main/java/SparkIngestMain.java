import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.delta.tables.DeltaTable;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import schema.Event;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static org.apache.spark.sql.functions.*;

public class SparkIngestMain {

    @Parameter(names={"--env", "-e"})
    private String env = "local";

    @Parameter(names={"-orgs", "-o"})
    private List<String> organizations = new ArrayList<>();

    private static final Logger logger = LogManager.getLogger("Main");

    public static void main(String[] args) {
        SparkIngestMain main = new SparkIngestMain();
        JCommander.newBuilder()
                .addObject(main)
                .build()
                .parse(args);

        main.run();
    }

    private void run() {
        final Properties config = getConfig();
        boolean isProd = env.equals("prod");

        var storageAccountName = (String) config.get("account_name");
        var applicationId = (String) config.get("app_id");
        var directoryId = (String) config.get("directory_id");
        var password = (String) config.get("password");
        final SparkSession.Builder builder = SparkSession.builder()
                .config("spark.ui.enabled", "false")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .appName("Github Events Ingestion");
        if (isProd) {
            // https://docs.delta.io/latest/delta-storage.html#configuration-adls-gen2
            builder.config("fs.azure.account.auth.type." + storageAccountName + ".dfs.core.windows.net", "OAuth")
                    .config("fs.azure.account.oauth.provider.type." + storageAccountName + ".dfs.core.windows.net",
                    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
                    .config("fs.azure.account.oauth2.client.id."+ storageAccountName + ".dfs.core.windows.net",
                            applicationId)
                    .config("fs.azure.account.oauth2.client.secret."+ storageAccountName + ".dfs.core.windows.net",
                            password)
                    .config("fs.azure.account.oauth2.client.endpoint."+ storageAccountName + ".dfs.core.windows.net",
                            "https://login.microsoftonline.com/"+ directoryId + "/oauth2/token");
        }
        final SparkSession spark = builder
                .getOrCreate();

        final PublicEventsAPI api = new PublicEventsAPI((String) config.get("token"));
        final String path = isProd ? "abfss://orgevents@" + storageAccountName + ".dfs.core.windows.net" : "/tmp";
        if (organizations.isEmpty()) {
            logger.info("No organization found in commandline arguments, defaulting to Microsoft");
            writeOrgEvents("Microsoft", path, api, spark);
        }
        for (String org: organizations) {
            writeOrgEvents(org, path, api, spark);
        }
    }

    private static Properties getConfig() {
        Properties prop = new Properties();
        try (InputStream in = SparkIngestMain.class.getResourceAsStream("settings.conf")) {
            prop.load(in);
            return prop;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Sometimes GitHub org is not the same as org name in the database.
     * Given the GitHub org name, map to correct name in delta table
     *
     * @param org GitHub org name
     * @return org name in database
     */
    private static String getDatabaseOrgName(String org) {
        return org.equals("facebook") ? "meta" : org;
    }

    private static void writeOrgEvents(
            String organization, final String path, final PublicEventsAPI api, final SparkSession spark) {
        try {
            var fullPath = path + "/org_events";
            LatestIDWriter idWriter = new LatestIDWriter((organization));
            List<Event> list;
            var exists = DeltaTable.isDeltaTable(fullPath);
            if (exists) {
                var latestIdOptional = idWriter.readLatestID();
                long latestId;
                if (latestIdOptional.isEmpty()) {
                    Dataset<Row> rdf = spark.read().format("delta").load(fullPath);
                    var latestRow = rdf
                            .where(rdf.col("org").equalTo(getDatabaseOrgName(organization)))
                            .orderBy(desc("id")).limit(1).first();
                    latestId = latestRow.getLong(latestRow.fieldIndex("id"));
                } else {
                    latestId = latestIdOptional.get();
                }
                list = api.getOrganizationEvents(organization, latestId);
                if (!list.isEmpty()) {
                    var df = spark.createDataFrame(list, Event.class)
                            .withColumn("org", lit(getDatabaseOrgName(organization)));
                    df.show();
                    DeltaTable deltaTable = DeltaTable.forPath(fullPath);
                    deltaTable.as("oldData")
                            .merge(
                                    df.as("newData"),
                                    "oldData.id = newData.id")
                            .whenMatched()
                            .updateExpr(
                                    new HashMap<>() {{
                                        put("id", "newData.id");
                                        put("createdAt", "newData.createdAt");
                                        put("date", "newData.date");
                                        put("repository", "newData.repository");
                                        put("type", "newData.type");
                                        put("user", "newData.user");
                                        put("org", "newData.org");
                                    }})
                            .whenNotMatched()
                            .insertExpr(
                                    new HashMap<>() {{
                                        put("id", "newData.id");
                                        put("createdAt", "newData.createdAt");
                                        put("date", "newData.date");
                                        put("repository", "newData.repository");
                                        put("type", "newData.type");
                                        put("user", "newData.user");
                                        put("org", "newData.org");
                                    }})
                            .execute();
                }
            } else {
                list = api.getOrganizationEvents(organization);
                var df = spark
                        .createDataFrame(list, Event.class)
                        .withColumn("org", lit(organization.equals("facebook") ? "meta" : organization));
                df.write().format("delta").partitionBy("date").save(fullPath);
            }
            if (!list.isEmpty()) {
                idWriter.writeLatestID(list.get(0).getId());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
