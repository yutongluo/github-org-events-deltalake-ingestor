import org.kohsuke.github.GHEvent;
import org.kohsuke.github.GHEventInfo;
import org.kohsuke.github.GHEventPayload;
import schema.Event;

import java.io.IOException;
import java.util.Date;
import java.util.List;

public class TestMain {
    public static void main(String[] args) {
        PublicEventsAPI api = new PublicEventsAPI();
        try {
            List<Event> list = api.getOrganizationEvents("Microsoft");
            for(Event event : list) {
                System.out.println(event);
            }
            System.out.println(list.size());
//            Event e = new Event(5L, "test", GHEvent.CREATE, new Date(), "me");
//            System.out.println(e);
//            throw new IOException();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
