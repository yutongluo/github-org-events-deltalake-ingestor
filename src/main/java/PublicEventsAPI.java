import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kohsuke.github.GHEventInfo;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.GitHubBuilder;
import org.kohsuke.github.PagedIterator;
import schema.Event;

public class PublicEventsAPI {

  private static final int PAGE_SIZE = 100;
  private static final Logger LOGGER = LogManager.getLogger(PublicEventsAPI.class);
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
    LOGGER.info("Retrieved " + events.size() + " events");
    List<Event> list = new ArrayList<>();
    for (GHEventInfo event : events) {
      Event e = new Event(event.getId(), event.getRepoName(), event.getType().name(),
          new Timestamp(event.getCreatedAt().getTime()), event.getActorLogin());
      list.add(e);
    }
    return list;
  }

  public List<Event> getOrganizationEvents(final String org, final long untilId)
      throws IOException {
    LOGGER.info("Retrieving events until id {} is reached.", untilId);
    long earliestId = Long.MAX_VALUE;
    final PagedIterator<GHEventInfo> iterator = github.getOrganization(org).listEvents()
        .withPageSize(PAGE_SIZE).iterator();
    List<Event> list = new ArrayList<>();
    while (iterator.hasNext() && earliestId > untilId) {
      var events = iterator.nextPage();
      for (GHEventInfo event : events) {
        Event e = new Event(event.getId(), event.getRepoName(), event.getType().name(),
            new Timestamp(event.getCreatedAt().getTime()), event.getActorLogin());
        list.add(e);
      }
      // records are in descending order according to date, meaning earliest record is the last row.
      earliestId = events.get(events.size() - 1).getId();
      LOGGER.info("Retrieved {} events up to {}.", events.size(), earliestId);
    }
    LOGGER.info("Retrieved total {} events.", list.size());
    return list;
  }
}
