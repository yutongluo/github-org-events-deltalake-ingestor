import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kohsuke.github.*;
import schema.Event;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.sql.Timestamp;

public class PublicEventsAPI {

    private static final Logger logger = LogManager.getLogger(PublicEventsAPI.class);
    public static final int PAGE_SIZE = 100;

    private final GitHub github;

    public PublicEventsAPI(final String apiToken) {
        try {
            github = new GitHubBuilder().withJwtToken(apiToken).build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Event> getOrganizationEvents(String org) throws IOException {
        var events = github.getOrganization(org).listEvents().toList();
        logger.info("Retrieved " + events.size() + " events");
        List<Event> list = new ArrayList<>();
        for (GHEventInfo event : events) {
            Event e = new Event(event.getId(), event.getRepoName(), event.getType().name(), new Timestamp(event.getCreatedAt().getTime()), event.getActorLogin());
            list.add(e);
        }
        return list;
    }

    public List<Event> getOrganizationEvents(final String org, final long untilId) throws IOException {
        logger.info("Retrieving events until id {} is reached.", untilId);
        long earliestId = Long.MAX_VALUE;
        final PagedIterator<GHEventInfo> iterator = github.getOrganization(org).listEvents().withPageSize(PAGE_SIZE).iterator();
        List<Event> list = new ArrayList<>();
        while (iterator.hasNext() && earliestId > untilId) {
            var events = iterator.nextPage();
            for (GHEventInfo event : events) {
                Event e = new Event(event.getId(), event.getRepoName(), event.getType().name(), new Timestamp(event.getCreatedAt().getTime()), event.getActorLogin());
                list.add(e);
            }
            // records are in descending order according to date, meaning earliest record is the last row.
            earliestId = events.get(events.size() - 1).getId();
            logger.info("Retrieved {} events up to {}.", events.size(), earliestId);
        }
        logger.info("Retrieved total {} events.", list.size());
        return list;
    }
}
