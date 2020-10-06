package net.sansa_stack.datalake.spark.model;

/**
 * Created by mmami on 10.10.16.
 */
import java.io.Serializable;

public class Triple implements Serializable {
    public String subject;
    public String property;
    public Object object;

    public Triple(String subject, String property, Object newobject) {
        this.subject = subject;
        this.property = property;
        this.object = newobject;
    }

    public String getSubject() {
        return subject;
    }
    public void setSubject(String subject) {
        this.subject = subject;
    }
    public String getProperty() {
        return property;
    }
    public void setProperty(String property) {
        this.property = property;
    }
    public Object getObject() {
        return object;
    }
    public void setObject(String object) {
        this.object = object;
    }

}
