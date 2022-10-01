package org.ohnlp.cat.api.cohorts;

import java.io.Serializable;
import java.util.Set;

public class CandidateScore implements Serializable {
    private String patientUID;
    private Set<String> evidenceIDs;
    private Double score;

    private int dataSourceCount = 1;

    public CandidateScore() {}

    public CandidateScore(String patientUID, Double score, Set<String> evidenceIDs) {
        this.patientUID = patientUID;
        this.score = score;
        this.evidenceIDs = evidenceIDs;
    }

    public String getPatientUID() {
        return patientUID;
    }

    public void setPatientUID(String patientUID) {
        this.patientUID = patientUID;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    public Set<String> getEvidenceIDs() {
        return evidenceIDs;
    }

    public void setEvidenceIDs(Set<String> evidenceIDs) {
        this.evidenceIDs = evidenceIDs;
    }

    public int getDataSourceCount() {
        return dataSourceCount;
    }

    public void setDataSourceCount(int dataSourceCount) {
        this.dataSourceCount = dataSourceCount;
    }
}