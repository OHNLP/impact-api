package org.ohnlp.cat.api.criteria.parser;

import java.util.Objects;

public class DataSourceRepresentation {
    private String sourceUMLSCUI;
    private String representation;
    private String representationDescription;
    private String resolverID;

    public String getSourceUMLSCUI() {
        return sourceUMLSCUI;
    }

    public void setSourceUMLSCUI(String sourceUMLSCUI) {
        this.sourceUMLSCUI = sourceUMLSCUI;
    }

    public String getRepresentation() {
        return representation;
    }

    public void setRepresentation(String representation) {
        this.representation = representation;
    }

    public String getRepresentationDescription() {
        return representationDescription;
    }

    public void setRepresentationDescription(String representationDescription) {
        this.representationDescription = representationDescription;
    }

    public String getResolverID() {
        return resolverID;
    }

    public void setResolverID(String resolverID) {
        this.resolverID = resolverID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataSourceRepresentation that = (DataSourceRepresentation) o;
        return Objects.equals(sourceUMLSCUI, that.sourceUMLSCUI) && Objects.equals(representation, that.representation) && Objects.equals(representationDescription, that.representationDescription) && Objects.equals(resolverID, that.resolverID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceUMLSCUI, representation, representationDescription, resolverID);
    }
}
