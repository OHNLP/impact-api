package org.ohnlp.cat.api.criteria.parser;

import java.util.Objects;

public class DataSourceRepresentation {
    private String dataSourceID;
    private String sourceUMLSCUI;
    private String representation;
    private String representationDescription;

    public String getDataSourceID() {
        return dataSourceID;
    }

    public void setDataSourceID(String dataSourceID) {
        this.dataSourceID = dataSourceID;
    }

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataSourceRepresentation that = (DataSourceRepresentation) o;
        return Objects.equals(dataSourceID, that.dataSourceID) && Objects.equals(sourceUMLSCUI, that.sourceUMLSCUI) && Objects.equals(representation, that.representation) && Objects.equals(representationDescription, that.representationDescription);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataSourceID, sourceUMLSCUI, representation, representationDescription);
    }
}
