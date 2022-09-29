package org.ohnlp.cat.api.criteria;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.codehaus.jackson.map.annotate.JsonTypeResolver;
import org.hl7.fhir.r4.model.DomainResource;
import org.ohnlp.cat.api.cohorts.CandidateScore;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "nodeType",
        visible = true
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = LogicalCriterion.class, name = "LOGICAL"),
        @JsonSubTypes.Type(value = EntityCriterion.class, name = "ENTITY")
})
public abstract class Criterion implements Serializable {
    private UUID nodeUID;
    private String title;
    private String description;
    private String nodeType;

    public abstract boolean matches(DomainResource resource);

    public abstract double score(Map<UUID, CandidateScore> scoreByCriterionUID);

    public UUID getNodeUID() {
        return nodeUID;
    }

    public void setNodeUID(UUID nodeUID) {
        this.nodeUID = nodeUID;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getNodeType() {
        return nodeType;
    }

    public void setNodeType(String nodeType) {
        this.nodeType = nodeType;
    }
}
