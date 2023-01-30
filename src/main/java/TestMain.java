import org.kohsuke.github.GHEvent;
import org.kohsuke.github.GHEventInfo;
import org.kohsuke.github.GHEventPayload;
import schema.Event;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class TestMain {
    public static void main(String[] args) {
        Properties prop = new Properties();
        try (InputStream in = SparkIngestMain.class.getResourceAsStream("settings.conf")) {
            prop.load(in);
            PublicEventsAPI api = new PublicEventsAPI((String) prop.get("token"));;
            List<Event> list = api.getOrganizationEvents("Microsoft", 26734333844L);
            for(Event event : list) {
                System.out.println(event);
            }
            System.out.println(list.size());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
