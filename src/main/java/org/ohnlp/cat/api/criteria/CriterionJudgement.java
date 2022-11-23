package org.ohnlp.cat.api.criteria;

import java.io.Serializable;

public enum CriterionJudgement implements Serializable {
    JUDGED_MATCH,
    JUDGED_MISMATCH,
    EVIDENCE_FOUND,
    EVIDENCE_FOUND_NLP,
    NO_EVIDENCE_FOUND,
    UNJUDGED
}
