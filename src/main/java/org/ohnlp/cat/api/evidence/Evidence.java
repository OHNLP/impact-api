package org.ohnlp.cat.api.evidence;

import java.io.Serializable;

public class Evidence implements Serializable {
    private String evidenceUID;
    private double score;

    public String getEvidenceUID() {
        return evidenceUID;
    }

    public void setEvidenceUID(String evidenceUID) {
        this.evidenceUID = evidenceUID;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }
}
