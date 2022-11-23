package org.ohnlp.cat.api.ehr;

public class DataSourceInformation {
    private String name;
    private String description;
    private String backendID;

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
}
