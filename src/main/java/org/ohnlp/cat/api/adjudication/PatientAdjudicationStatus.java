package org.ohnlp.cat.api.adjudication;

import org.ohnlp.cat.api.cohorts.CandidateInclusion;
import org.ohnlp.cat.api.criteria.CriterionJudgement;

import java.io.Serializable;
import java.util.Map;

public class PatientAdjudicationStatus implements Serializable {
    private int numAdjudicators;
    private Map<CriterionJudgement, Integer> status;
    private CriterionJudgement tiebreakerOverride;

    public int getNumAdjudicators() {
        return numAdjudicators;
    }

    public void setNumAdjudicators(int numAdjudicators) {
        this.numAdjudicators = numAdjudicators;
    }

    public Map<CriterionJudgement, Integer> getStatus() {
        return status;
    }

    public void setStatus(Map<CriterionJudgement, Integer> status) {
        this.status = status;
    }

    public CriterionJudgement getTiebreakerOverride() {
        return tiebreakerOverride;
    }

    public void setTiebreakerOverride(CriterionJudgement tiebreakerOverride) {
        this.tiebreakerOverride = tiebreakerOverride;
    }
}
