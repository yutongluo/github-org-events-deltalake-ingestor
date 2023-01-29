package schema;

import org.kohsuke.github.GHEvent;

import java.util.Date;

public class Event {
    public long id;
    public String repository;
    public GHEvent type;
    public Date createdAt;
    public String user;

    public Event() {

    }

    public Event(long id, String repository, GHEvent type, Date createdAt, String user) {
        this.id = id;
        this.repository = repository;
        this.type = type;
        this.createdAt = createdAt;
        this.user = user;
    }

    @Override
    public String toString() {
        return String.format("Id: %d, Repo: %s, Type: %s, Date: %tc, User: %s", id, repository, type, createdAt, user);
    }
}
