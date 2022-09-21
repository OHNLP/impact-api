package org.ohnlp.cat.api.criteria;

public class CriterionInfo {
    private CriterionJudgement judgement;
    private String comment;

    public CriterionJudgement getJudgement() {
        return judgement;
    }

    public void setJudgement(CriterionJudgement judgement) {
        this.judgement = judgement;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }
}
