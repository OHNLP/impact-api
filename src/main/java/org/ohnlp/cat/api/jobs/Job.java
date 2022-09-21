package org.ohnlp.cat.api.jobs;

import java.util.Date;
import java.util.UUID;

public class Job {
    private UUID projectUID;
    private UUID jobUID;
    private Date startDate;
    private JobStatus status;

    public UUID getProjectUID() {
        return projectUID;
    }

    public void setProjectUID(UUID projectUID) {
        this.projectUID = projectUID;
    }

    public UUID getJobUID() {
        return jobUID;
    }

    public void setJobUID(UUID jobUID) {
        this.jobUID = jobUID;
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public JobStatus getStatus() {
        return status;
    }

    public void setStatus(JobStatus status) {
        this.status = status;
    }
}
