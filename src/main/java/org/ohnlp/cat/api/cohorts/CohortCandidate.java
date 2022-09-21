package org.ohnlp.cat.api.cohorts;

public class CohortCandidate {
    private String patUID;
    private CandidateInclusion inclusion;

    public String getPatUID() {
        return patUID;
    }

    public void setPatUID(String patUID) {
        this.patUID = patUID;
    }


    public CandidateInclusion getInclusion() {
        return inclusion;
    }

    public void setInclusion(CandidateInclusion inclusion) {
        this.inclusion = inclusion;
    }
}
