package org.ohnlp.cat.common.impl.ehr;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.hl7.fhir.r4.model.*;
import org.ohnlp.cat.api.criteria.ClinicalEntityType;
import org.ohnlp.cat.api.criteria.FHIRValueLocationPath;
import org.ohnlp.cat.api.ehr.ResourceProvider;

import java.nio.charset.StandardCharsets;
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
        String base = "SELECT nlp.note_nlp_id, c.concept_name, nlp.note_nlp_concept_id, " +
                "n.person_id, n.note_id, n.note_date, " +
                "n.note_text, nlp.offset, nlp.lexical_variant " +
                "FROM " + cdmSchemaName + ".NOTE n JOIN " + cdmSchemaName + ".NOTE_NLP nlp " +
                "ON n.note_id = nlp.note_id AND nlp.term_exists = 'Y' " +
                "JOIN " + cdmSchemaName + ".CONCEPT c " +
                "ON nlp.note_nlp_concept_id = c.concept_id WHERE c.domain_id = ";
        String domainID;
        switch (type) {
            case PERSON:
                domainID = "'NONE'";
                break;
            case CONDITION:
                domainID = "'Condition'";
                break;
            case PROCEDURE:
                domainID = "'Procedure'";
                break;
            case MEDICATION:
                domainID = "'Drug'";
                break;
            case OBSERVATION:
                domainID = "'Measurement'";
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
                Schema.Field.of("note_nlp_id", Schema.FieldType.INT32),
                Schema.Field.of("concept_name", Schema.FieldType.STRING),
                Schema.Field.of("note_nlp_concept_id", Schema.FieldType.INT32),
                Schema.Field.of("person_id", Schema.FieldType.INT32),
                Schema.Field.of("note_id", Schema.FieldType.STRING),
                Schema.Field.of("note_date", Schema.FieldType.DATETIME),
                Schema.Field.of("note_text", Schema.FieldType.STRING),
                Schema.Field.of("offset", Schema.FieldType.STRING),
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

    @Override
    public String getPathForValueReference(FHIRValueLocationPath valueRef) {
        switch (valueRef) {
            case PERSON_ID:
            case PERSON_GENDER:
            case PERSON_DOB:
            case CONDITION_CODE:
            case PROCEDURE_CODE:
            case MEDICATION_CODE:
            case OBSERVATION_CODE:
            case OBSERVATION_VALUE:
                break;
            default:
                throw new UnsupportedOperationException("Unsupported type " + valueRef);
        }
        return String.join(".", "contained", valueRef.getPath());
    }

    @Override
    public String extractPatUIDForResource(ClinicalEntityType type, DomainResource resource) {
        // First, unbox
        DomainResource r = (DomainResource) resource.getContained().get(0);
        // Now find type
        switch (type) {
            case PERSON: {
                String base = r.getId();
                // remove source identifier
                base = base.substring(base.indexOf(":") + 1);
                // remove type identifier
                base = base.substring(base.indexOf(":") + 1);
                return base;
            }
            case CONDITION:
                return ((Condition) r).getSubject().getIdentifier().getValue();
            case PROCEDURE:
                return ((Procedure) r).getSubject().getIdentifier().getValue();
            case MEDICATION:
                return ((MedicationStatement) r).getSubject().getIdentifier().getValue();
            case OBSERVATION:
                return ((Observation) r).getSubject().getIdentifier().getValue();
            default:
                throw new UnsupportedOperationException("Unknown entity type " + type);
        }
    }

    // Row to Resource Mapping functions
    private final SerializableFunction<Row, DomainResource> personMappingFunction = (in) -> {
        return new Person(); // There is no person information supported in note_nlp at this time
    };

    private final SerializableFunction<Row, DomainResource> conditionMappingFunction = (in) -> {
        String recordID = in.getInt32("note_nlp_id") + "";
        String personID = in.getInt32("person_id") + "";
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
        cdn.addExtension().setId("nlp_meta")
                .setProperty("offset", new StringType(in.getString("offset")))
                .setProperty("text", new StringType(in.getString("lexical_variant")));

        String note_id = in.getString("note_id");
        String note_text = in.getString("note_text");
        DocumentReference docRef = new DocumentReference();
        docRef.setMasterIdentifier(new Identifier().setValue(note_id + ""));
        docRef.setDate(dtm);
        docRef.setId(String.join(":", sourceName, ClinicalEntityType.CONDITION.name(), recordID));
        docRef.addContent().setAttachment(new Attachment().setData(note_text.getBytes(StandardCharsets.UTF_8)));
        docRef.addContained(cdn);
        return docRef;
    };

    private final SerializableFunction<Row, DomainResource> medicationMappingFunction = (in) -> {
        String recordID = in.getInt32("note_nlp_id") + "";
        String personID = in.getInt32("person_id") + "";
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
        ms.addExtension().setId("nlp_meta")
                .setProperty("offset", new StringType(in.getString("offset")))
                .setProperty("text", new StringType(in.getString("lexical_variant")));

        String note_id = in.getString("note_id");
        String note_text = in.getString("note_text");
        DocumentReference docRef = new DocumentReference();
        docRef.setMasterIdentifier(new Identifier().setValue(note_id + ""));
        docRef.setDate(dtm);
        docRef.setId(String.join(":", sourceName, ClinicalEntityType.MEDICATION.name(), recordID));
        docRef.addContent().setAttachment(new Attachment().setData(note_text.getBytes(StandardCharsets.UTF_8)));
        docRef.addContained(ms);
        return docRef;
    };

    private final SerializableFunction<Row, DomainResource> procedureMappingFUnction = (in) -> {
        String recordID = in.getInt32("note_nlp_id") + "";
        String personID = in.getInt32("person_id") + "";
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
        prc.addExtension().setId("nlp_meta")
                .setProperty("offset", new StringType(in.getString("offset")))
                .setProperty("text", new StringType(in.getString("lexical_variant")));

        String note_id = in.getString("note_id");
        String note_text = in.getString("note_text");
        DocumentReference docRef = new DocumentReference();
        docRef.setMasterIdentifier(new Identifier().setValue(note_id + ""));
        docRef.setDate(dtm);
        docRef.setId(String.join(":", sourceName, ClinicalEntityType.PROCEDURE.name(), recordID));
        docRef.addContent().setAttachment(new Attachment().setData(note_text.getBytes(StandardCharsets.UTF_8)));
        docRef.addContained(prc);
        return docRef;
    };

    private final SerializableFunction<Row, DomainResource> observationMappingFunction = (in) -> {
        String recordID = in.getInt32("note_nlp_id") + "";
        String personID = in.getInt32("person_id") + "";
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
        obs.addExtension().setId("nlp_meta")
                .setProperty("offset", new StringType(in.getString("offset")))
                .setProperty("text", new StringType(in.getString("lexical_variant")));

        String note_id = in.getString("note_id");
        String note_text = in.getString("note_text");
        DocumentReference docRef = new DocumentReference();
        docRef.setDate(dtm);
        docRef.setMasterIdentifier(new Identifier().setValue(note_id + ""));
        docRef.setId(String.join(":", sourceName, ClinicalEntityType.OBSERVATION.name(), recordID));
        docRef.addContent().setAttachment(new Attachment().setData(note_text.getBytes(StandardCharsets.UTF_8)));
        docRef.addContained(obs);
        return docRef;
    };
}
