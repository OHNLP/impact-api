package org.ohnlp.cat.api.projects;

import java.util.UUID;

public class ProjectRole {
    private String userUID;
    private UUID projectUID;
    private ProjectAuthorityGrant grant;

    public String getUserUID() {
        return userUID;
    }

    public void setUserUID(String userUID) {
        this.userUID = userUID;
    }

    public UUID getProjectUID() {
        return projectUID;
    }

    public void setProjectUID(UUID projectUID) {
        this.projectUID = projectUID;
    }

    public ProjectAuthorityGrant getGrant() {
        return grant;
    }

    public void setGrant(ProjectAuthorityGrant grant) {
        this.grant = grant;
    }
}
