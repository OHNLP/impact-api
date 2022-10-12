package org.ohnlp.cat.common.impl.ehr;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.hl7.fhir.r4.model.*;
import org.ohnlp.cat.api.criteria.ClinicalEntityType;
import org.ohnlp.cat.api.ehr.ResourceProvider;

import java.util.*;

public class OHDSICDMNLPResourceProvider implements ResourceProvider {
    private String cdmSchemaName;
    private String sourceName;

    @Override
    public void init(String sourceName, Map<String, Object> config) {
        this.sourceName = sourceName;
        this.cdmSchemaName = config.getOrDefault("schema", "cdm").toString();
    }

    @Override
    public String getQuery(ClinicalEntityType type) {
        String base = "SELECT nlp.note_nlp_id, c.concept_name, nlp.note_nlp_concept_id" +
                "n.person_id, n.note_id, n.note_date, " +
                "nlp.snippet, nlp.lexical_variant " +
                "FROM " + cdmSchemaName + ".NOTE n JOIN " + cdmSchemaName + ".NOTE_NLP nlp " +
                "ON n.note_id = nlp.note_id AND nlp.term_exists = 'Y' " +
                "JOIN " + cdmSchemaName + ".CONCEPT c " +
                "ON nlp.note_nlp_concept_id = c.concept_id WHERE c.domain_id = ";
        String domainID;
        switch (type) {
            case PERSON:
                domainID = "NONE";
                break;
            case CONDITION:
                domainID = "Condition";
                break;
            case PROCEDURE:
                domainID = "Procedure";
                break;
            case MEDICATION:
                domainID = "Drug";
                break;
            case OBSERVATION:
                domainID = "Measurement";
                break;
            default:
                throw new UnsupportedOperationException("Unknown entity type " + type);
        }
        return base + domainID;
    }

    @Override
    public Schema getQuerySchema(ClinicalEntityType type) {
        // Schema is the same across all return types
        return Schema.of(
                Schema.Field.of("note_nlp_id", Schema.FieldType.INT64),
                Schema.Field.of("concept_name", Schema.FieldType.STRING),
                Schema.Field.of("note_nlp_concept_id", Schema.FieldType.INT32),
                Schema.Field.of("person_id", Schema.FieldType.INT64),
                Schema.Field.of("note_id", Schema.FieldType.INT64),
                Schema.Field.of("note_date", Schema.FieldType.DATETIME),
                Schema.Field.of("snippet", Schema.FieldType.STRING),
                Schema.Field.of("lexical_variant", Schema.FieldType.STRING)
        );
    }

    @Override
    public String getEvidenceIDFilter(ClinicalEntityType type) {
        return "note_nlp_id = ?";
    }

    @Override
    public String getIndexableIDColumnName(ClinicalEntityType type) {
        return "note_nlp_id";
    }

    @Override
    public SerializableFunction<Row, DomainResource> getRowToResourceMapper(ClinicalEntityType type) {
        switch (type) {
            case PERSON:
                return personMappingFunction;
            case CONDITION:
                return conditionMappingFunction;
            case PROCEDURE:
                return procedureMappingFUnction;
            case MEDICATION:
                return medicationMappingFunction;
            case OBSERVATION:
                return observationMappingFunction;
            default:
                throw new UnsupportedOperationException("Unknown clinical entity type " + type);
        }
    }

    @Override
    public Set<String> convertToLocalTerminology(ClinicalEntityType type, String input) {
        return Collections.singleton(input); // TODO
    }

    @Override
    public Object[] parseIDTagToParams(ClinicalEntityType type, String evidenceUID) {
        return new Object[] {Long.parseLong(evidenceUID)};
    }

    // Row to Resource Mapping functions
    private final SerializableFunction<Row, DomainResource> personMappingFunction = (in) -> {
        return new Person(); // There is no person information supported in note_nlp at this time
    };

    private final SerializableFunction<Row, DomainResource> conditionMappingFunction = (in) -> {
        String recordID = in.getInt64("note_nlp_id") + "";
        String personID = in.getInt64("person_id") + "";
        String conditionConceptID = in.getInt32("note_nlp_concept_id") + "";
        Date dtm = new Date(in.getDateTime("note_date").getMillis());
        Condition cdn = new Condition();
        cdn.setId(String.join(":", sourceName, ClinicalEntityType.CONDITION.name(), recordID));
        cdn.setSubject(new Reference().setIdentifier(new Identifier().setValue(personID)));
        cdn.setCode(
                new CodeableConcept().addCoding(
                        new Coding(
                                "https://athena.ohdsi.org/",
                                conditionConceptID,
                                in.getString("concept_name")))
        );
        cdn.setRecordedDate(dtm);
        return cdn;
    };

    private final SerializableFunction<Row, DomainResource> medicationMappingFunction = (in) -> {
        String recordID = in.getInt64("note_nlp_id") + "";
        String personID = in.getInt64("person_id") + "";
        String drugConceptId = in.getInt32("note_nlp_concept_id") + "";
        Date dtm = new Date(in.getDateTime("note_date").getMillis());
        // TODO see about mapping date ends? there doesn't seem to currently be a target in FHIR somehow (or am just blind)
        MedicationStatement ms = new MedicationStatement();
        ms.setId(String.join(":", sourceName, ClinicalEntityType.MEDICATION.name(), recordID));
        ms.setSubject(new Reference().setIdentifier(new Identifier().setValue(personID)));
        ms.setMedication(
                new CodeableConcept().addCoding(
                        new Coding(
                                "https://athena.ohdsi.org/",
                                drugConceptId,
                                in.getString("concept_name")))
        );
        ms.setDateAsserted(dtm);
        return ms;
    };

    private final SerializableFunction<Row, DomainResource> procedureMappingFUnction = (in) -> {
        String recordID = in.getInt64("note_nlp_id") + "";
        String personID = in.getInt64("person_id") + "";
        String conceptID = in.getInt32("note_nlp_concept_id") + "";
        Date dtm = new Date(in.getDateTime("note_date").getMillis());
        Procedure prc = new Procedure();
        prc.setId(String.join(":", sourceName, ClinicalEntityType.PROCEDURE.name(), recordID));
        prc.setSubject(new Reference().setIdentifier(new Identifier().setValue(personID)));
        prc.setCode(
                new CodeableConcept().addCoding(
                        new Coding(
                                "https://athena.ohdsi.org/",
                                conceptID,
                                in.getString("concept_name")))
        );
        prc.setPerformed(new DateTimeType(dtm));
        return prc;
    };

    private final SerializableFunction<Row, DomainResource> observationMappingFunction = (in) -> {
        String recordID = in.getInt64("note_nlp_id") + "";
        String personID = in.getInt64("person_id") + "";
        String conceptID = in.getInt32("note_nlp_concept_id") + "";
        Date dtm = new Date(in.getDateTime("note_date").getMillis());
        Observation obs = new Observation();
        obs.setId(String.join(":", sourceName, ClinicalEntityType.OBSERVATION.name(), recordID));
        obs.setSubject(new Reference().setIdentifier(new Identifier().setValue(personID)));
        obs.setCode(
                new CodeableConcept().addCoding(
                        new Coding(
                                "https://athena.ohdsi.org/",
                                conceptID,
                                in.getString("concept_name")))
        );
        obs.setIssued(dtm);
        return obs;
    };
}
