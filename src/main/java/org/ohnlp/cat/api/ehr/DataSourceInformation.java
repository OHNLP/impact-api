package org.ohnlp.cat.api.ehr;

import java.util.List;

public class DataSourceInformation {
    private String name;
    private String description;
    private String backendID;
    private List<String> textResolvers;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getBackendID() {
        return backendID;
    }

    public void setBackendID(String backendID) {
        this.backendID = backendID;
    }

    public List<String> getTextResolvers() {
        return textResolvers;
    }

    public void setTextResolvers(List<String> textResolvers) {
        this.textResolvers = textResolvers;
    }
}
