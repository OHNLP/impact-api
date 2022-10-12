package org.ohnlp.cat.api.ehr;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.hl7.fhir.r4.model.DomainResource;
import org.ohnlp.cat.api.criteria.ClinicalEntityType;
import org.ohnlp.cat.api.criteria.FHIRValueLocationPath;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * Contains implementation required to FHIR resources from a data source.
 * <p>
 * Produced resources are expected to possess ID tags of the format source_system_name:type:evidence_uid
 */
public interface ResourceProvider extends Serializable {

    void init(String sourceName, Map<String, Object> config);

    String getQuery(ClinicalEntityType type);

    Schema getQuerySchema(ClinicalEntityType type);

    String getEvidenceIDFilter(ClinicalEntityType type);

    String getIndexableIDColumnName(ClinicalEntityType type);

    SerializableFunction<Row, DomainResource> getRowToResourceMapper(ClinicalEntityType type);

    Set<String> convertToLocalTerminology(ClinicalEntityType type, String input);

    Object[] parseIDTagToParams(ClinicalEntityType type, String evidenceUID);

    String getPathForValueReference(FHIRValueLocationPath valueRef);

    String extractPatUIDForResource(ClinicalEntityType type, DomainResource resource);
}
