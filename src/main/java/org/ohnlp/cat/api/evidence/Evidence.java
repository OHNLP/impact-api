package org.ohnlp.cat.api.evidence;

public class Evidence {
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
