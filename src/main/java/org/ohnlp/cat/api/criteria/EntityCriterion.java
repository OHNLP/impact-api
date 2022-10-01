package org.ohnlp.cat.api.criteria;

import org.hl7.fhir.r4.model.DomainResource;
import org.ohnlp.cat.api.cohorts.CandidateScore;

import java.util.Map;
import java.util.UUID;

/**
 * Represents an individual clinical entity, e.g. a certain diagnosis, observation, drug, etc.
 * that satisfies all the conditions listed in its {@link EntityValue} {@link #components}.
 * <p>
 * For scoring purposes, the entity as a whole is treated as a match/unmatch, as opposed to
 * individual components. If individual component behaviour is desired, consider using a
 * {@link LogicalCriterion} with a {@link LogicalCriterion#setType(LogicalRelationType)} of {@link LogicalRelationType#MIN_OR}
 * instead
 */
public class EntityCriterion extends Criterion {
    public ClinicalEntityType type;
    public EntityValue[] components;

    public ClinicalEntityType getType() {
        return type;
    }

    public void setType(ClinicalEntityType type) {
        this.type = type;
    }

    public EntityValue[] getComponents() {
        return components;
    }

    public void setComponents(EntityValue[] components) {
        this.components = components;
    }

    @Override
    public boolean matches(DomainResource resource) {
        for (EntityValue component : components) {
            if (!component.matches(resource)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public double score(Map<UUID, CandidateScore> scoreByCriterionUID) {
        return scoreByCriterionUID.containsKey(getNodeUID()) ?
                scoreByCriterionUID.get(getNodeUID()).getScore()/scoreByCriterionUID.get(getNodeUID()).getDataSourceCount()
                : 0.00;
    }
}
