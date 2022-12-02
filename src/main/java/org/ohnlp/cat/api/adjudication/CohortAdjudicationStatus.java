package org.ohnlp.cat.api.adjudication;

import org.ohnlp.cat.api.cohorts.CandidateInclusion;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

public class CohortAdjudicationStatus implements Serializable {
    private int numAdjudicators;
    private Map<CandidateInclusion, Integer> status;
    private CandidateInclusion tiebreakerOverride;

    public int getNumAdjudicators() {
        return numAdjudicators;
    }

    public void setNumAdjudicators(int numAdjudicators) {
        this.numAdjudicators = numAdjudicators;
    }

    public Map<CandidateInclusion, Integer> getStatus() {
        return status;
    }

    public void setStatus(Map<CandidateInclusion, Integer> status) {
        this.status = status;
    }

    public CandidateInclusion getTiebreakerOverride() {
        return tiebreakerOverride;
    }

    public void setTiebreakerOverride(CandidateInclusion tiebreakerOverride) {
        this.tiebreakerOverride = tiebreakerOverride;
    }
}
