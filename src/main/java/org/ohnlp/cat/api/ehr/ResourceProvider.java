package org.ohnlp.cat.api.ehr;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.hl7.fhir.r4.model.DomainResource;
import org.ohnlp.cat.api.criteria.ClinicalEntityType;
import org.ohnlp.cat.api.criteria.EntityValue;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

public interface ResourceProvider extends Serializable {

    void init(Map<String, Object> config);

    String getQuery(ClinicalEntityType type);
    Schema getQuerySchema(ClinicalEntityType type);
    String getEvidenceIDFilter(ClinicalEntityType type);
    String getIndexableIDColumnName(ClinicalEntityType type);
    SerializableFunction<Row, DomainResource> getRowToResourceMapper(ClinicalEntityType type);

    Set<EntityValue> convertToLocalTerminology(ClinicalEntityType type, EntityValue input);
    Object[] parseIDTagToParams(ClinicalEntityType type, String evidenceUID);
}
