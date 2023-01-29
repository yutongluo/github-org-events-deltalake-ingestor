import org.kohsuke.github.*;
import schema.Event;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class PublicEventsAPI {
    private final GitHub github;

    public PublicEventsAPI() {
        Properties prop = new Properties();
        try (InputStream in = getClass().getResourceAsStream("keys.conf")) {
            prop.load(in);
            String apiToken = (String) prop.get("token");
            github = new GitHubBuilder().withJwtToken(apiToken).build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Event> getOrganizationEvents(String org) throws IOException {
        var events = github.getOrganization(org).listEvents().toList();
        System.out.println("Retrieved " + events.size() + " events");
        List<Event> list = new ArrayList<>();
        for (GHEventInfo event : events) {
            Event e = new Event(event.getId(), event.getRepository().getName(), event.getType(), event.getCreatedAt(), event.getActorLogin());
            list.add(e);
        }
        return list;
    }
}
