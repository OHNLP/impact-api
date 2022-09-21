package org.ohnlp.cat.api.criteria;

import org.hl7.fhir.r4.model.DomainResource;
import org.ohnlp.cat.api.cohorts.CandidateScore;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

public abstract class Criterion implements Serializable {
    private UUID nodeUID;

    public abstract boolean matches(DomainResource resource);
    public abstract double score(Map<UUID, CandidateScore> scoreByCriterionUID);

    public UUID getNodeUID() {
        return nodeUID;
    }

    public void setNodeUID(UUID nodeUID) {
        this.nodeUID = nodeUID;
    }
}
