package schema;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Timestamp;

public class Event implements Serializable {
    public long id;
    public String repository;
    public String type;
    public Timestamp createdAt;

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public Date date;
    public String user;

    public Event() {

    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getRepository() {
        return repository;
    }

    public void setRepository(String repository) {
        this.repository = repository;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Timestamp getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Timestamp createdAt) {
        this.createdAt = createdAt;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public Event(long id, String repository, String type, Timestamp createdAt, String user) {
        this.id = id;
        this.repository = repository;
        this.type = type;
        this.createdAt = createdAt;
        this.date = new Date(createdAt.getTime());
        this.user = user;
    }

    @Override
    public String toString() {
        return String.format("Id: %d, Repo: %s, Type: %s, Date: %tc, User: %s", id, repository, type, createdAt, user);
    }
}
