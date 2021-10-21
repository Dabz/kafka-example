package io.confluent.dabz;

import com.fasterxml.jackson.annotation.*;
import io.confluent.kafka.schemaregistry.annotations.Schema;
import io.confluent.kafka.schemaregistry.annotations.SchemaReference;
import org.apache.commons.compress.utils.IOUtils;
import org.w3c.dom.Text;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

@Schema(value = new TextResource();
        refs = {
                @SchemaReference(name = "School.schema.json", subject = "school", version = 4)
        })
public class Student {
    private Integer id;
    private String name;

    private School school;

    public Student() {
    }

    public Student(Integer id, String name, School school) {
        this.id = id;
        this.name = name;
        this.school = school;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public School getSchool() {
        return school;
    }

    public void setSchool(School school) {
        this.school = school;
    }
}