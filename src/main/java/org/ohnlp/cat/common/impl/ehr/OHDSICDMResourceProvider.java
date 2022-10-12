package org.ohnlp.cat.common.impl.ehr;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.hl7.fhir.r4.model.*;
import org.ohnlp.cat.api.criteria.ClinicalEntityType;
import org.ohnlp.cat.api.ehr.ResourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class OHDSICDMResourceProvider implements ResourceProvider {

    private String cdmSchemaName;
    private Connection conn;
    private String sourceName;

    @Override
    public void init(String sourceName, Map<String, Object> config) {
        this.sourceName = sourceName;
        this.cdmSchemaName = config.getOrDefault("schema", "cdm").toString();
    }

    @Override
    public String getQuery(ClinicalEntityType type) {
        switch (type) {
            case PERSON:
                return "SELECT person_id, " +
                        "gender_concept_id, " +
                        "year_of_birth, " +
                        "month_of_birth," +
                        "day_of_birth," +
                        "race_concept_id," +
                        "ethnicity_concept_id" +
                        " FROM " + cdmSchemaName + ".person ";
            case CONDITION:
                return "SELECT co.condition_occurrence_id, " +
                        "co.person_id, " +
                        "co.condition_concept_id, " +
                        "c.concept_name," +
                        "co.condition_start_date FROM " +
                        cdmSchemaName + ".condition_occurrence co JOIN " + cdmSchemaName + ".concept c " +
                        "ON co.condition_concept_id = c.concept_id";
            case PROCEDURE:
                return "SELECT p.procedure_occurrence_id, " +
                        "p.person_id, " +
                        "p.procedure_concept_id, " +
                        "c.concept_name, " +
                        "p.procedure_date FROM " +
                        cdmSchemaName + ".procedure_occurrence p JOIN " + cdmSchemaName + ".concept c " +
                        "ON p.procedure_concept_id = c.concept_id";
            case MEDICATION:
                return "SELECT d.drug_exposure_id, " +
                        "d.person_id, " +
                        "d.drug_concept_id, " +
                        "c.concept_name, " +
                        "d.drug_exposure_start_date," +
                        "d.drug_exposure_end_date FROM " +
                        cdmSchemaName + ".drug_exposure d JOIN " + cdmSchemaName + ".concept c " +
                        "ON d.drug_concept_id = c.concept_id";
            case OBSERVATION:
                return "SELECT m.measurement_id, " +
                        "m.person_id, " +
                        "m.measurement_concept_id, " +
                        "c.concept_name, " +
                        "m.measurement_date," +
                        "m.value_as_number," +
                        "m.value_as_concept_id FROM " +
                        cdmSchemaName + ".measurement m JOIN " + cdmSchemaName + ".concept c " +
                        "ON m.measurement_concept_id = c.concept_id";
            default:
                throw new UnsupportedOperationException("Unknown clinical entity type " + type);
        }
    }

    @Override
    public Schema getQuerySchema(ClinicalEntityType type) {
        switch (type) {
            case PERSON:
                return personSchema;
            case CONDITION:
                return conditionSchema;
            case PROCEDURE:
                return procedureSchema;
            case MEDICATION:
                return medicationSchema;
            case OBSERVATION:
                return observationSchema;
            default:
                throw new UnsupportedOperationException("Unknown clinical entity type " + type);
        }
    }

    @Override
    public String getEvidenceIDFilter(ClinicalEntityType type) {
        switch (type) {
            case PERSON:
                return "person_id = ?";
            case CONDITION:
                return "condition_occurrence_id = ?";
            case PROCEDURE:
                return "procedure_occurrence_id = ?";
            case MEDICATION:
                return "drug_exposure_id = ?";
            case OBSERVATION:
                return "measurement_id = ?";
            default:
                throw new UnsupportedOperationException("Unknown clinical entity type " + type);
        }
    }

    @Override
    public String getIndexableIDColumnName(ClinicalEntityType type) {
        switch (type) {
            case PERSON:
                return "person_id";
            case CONDITION:
                return "condition_occurrence_id";
            case PROCEDURE:
                return "procedure_occurrence_id";
            case MEDICATION:
                return "drug_exposure_id";
            case OBSERVATION:
                return "measurement_id";
            default:
                throw new UnsupportedOperationException("Unknown clinical entity type " + type);
        }
    }

    @Override
    public SerializableFunction<Row, DomainResource> getRowToResourceMapper(ClinicalEntityType type) {
        switch (type) {
            case PERSON:
                return personMappingFunction;
            case CONDITION:
                return conditionMappingFunction;
            case PROCEDURE:
                return procedureMappingFunction;
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
        Set<String> resultCodes = new HashSet<>();
        // Only EQ and IN operations are supported for _CODE paths, so we can just flat expand
        // the resulting values
        try {
            PreparedStatement lookupPS = conn.prepareStatement("SELECT concept_id FROM cdm.CONCEPT WHERE concept_code = ?");
            lookupPS.setString(1, input.toUpperCase(Locale.ROOT));
            ResultSet rs = lookupPS.executeQuery();
            while (rs.next()) {
                resultCodes.add(rs.getInt("concept_id") + "");
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e); // TODO
        }
        if (resultCodes.size() == 0) {
            throw new RuntimeException("Vocab expansion resulted in 0 terms!"); // TODO
        }
        return resultCodes;
    }

    @Override
    public Object[] parseIDTagToParams(ClinicalEntityType type, String evidenceUID) {
        return new Object[] {Long.parseLong(evidenceUID)}; // TODO not all types might be long
    }

    // Row to Resource Mapping functions
    private final SerializableFunction<Row, DomainResource> personMappingFunction = (in) -> {
        String personID = in.getInt32("person_id") + "";
        int genderConceptId = in.getInt32("gender_concept_id");
        int birthyr = in.getInt32("year_of_birth");
        int birthmnth = in.getInt32("month_of_birth");
        int birthday = in.getInt32("day_of_birth");
        int raceConceptId = in.getInt32("race_concept_id");
        int ethnicityConceptId = in.getInt32("ethnicity_concept_id");
        Person p = new Person();
        p.setId(String.join(":", sourceName, ClinicalEntityType.PERSON.name(), personID));
        switch (genderConceptId) {
            case 0:
                p.setGender(Enumerations.AdministrativeGender.NULL);
                break;
            case 8507:
                p.setGender(Enumerations.AdministrativeGender.MALE);
                break;
            case 8532:
                p.setGender(Enumerations.AdministrativeGender.FEMALE);
                break;
            default:
                p.setGender(Enumerations.AdministrativeGender.UNKNOWN);
                break;
        }
        p.setGender(genderConceptId == 0 ? // Only accepted values are 8507/8532, leave 0 as null fallback
                Enumerations.AdministrativeGender.NULL :
                (genderConceptId == 8507 ?
                        Enumerations.AdministrativeGender.MALE :
                        Enumerations.AdministrativeGender.FEMALE));
        p.setBirthDate(new GregorianCalendar(birthyr, birthmnth - 1, birthday).getTime());
        // TODO seems race and ethnicity not mapped to FHIR person? investigate where else this is.
        return p;
    };

    private final SerializableFunction<Row, DomainResource> conditionMappingFunction = (in) -> {
        String recordID = in.getInt32("condition_occurrence_id") + "";
        String personID = in.getInt32("person_id") + "";
        String conditionConceptID = in.getInt32("condition_concept_id") + "";
        Date dtm = new Date(in.getDateTime("condition_start_date").getMillis());
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
        String recordID = in.getInt32("drug_exposure_id") + "";
        String personID = in.getInt32("person_id") + "";
        String drugConceptId = in.getInt32("drug_concept_id") + "";
        Date dtm = new Date(in.getDateTime("drug_exposure_start_date").getMillis());
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

    private final SerializableFunction<Row, DomainResource> procedureMappingFunction = (in) -> {
        String recordID = in.getInt32("procedure_occurrence_id") + "";
        String personID = in.getInt32("person_id") + "";
        String conceptID = in.getInt32("procedure_concept_id") + "";
        Date dtm = new Date(in.getDateTime("procedure_date").getMillis());
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
        String recordID = in.getInt32("measurement_id") + "";
        String personID = in.getInt32("person_id") + "";
        String conceptID = in.getInt32("measurement_concept_id") + "";
        Date dtm = new Date(in.getDateTime("measurement_date").getMillis());
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
        String value = null;
        if (in.getFloat("value_as_number") != null) {
            value = in.getFloat("value_as_number") + "";
        } else {
            value = in.getString("value_as_concept_id");
            if (value != null && value.trim().length() == 0) {
                value = null;
            }
        }
        if (value != null) {
            obs.setValue(new StringType(value));
        }
        obs.setIssued(dtm);
        return obs;
    };

    // Individual query result schemas
    private final Schema personSchema = Schema.builder()
            .addFields(
                    Schema.Field.of("person_id", Schema.FieldType.INT32),
                    Schema.Field.of("gender_concept_id", Schema.FieldType.INT32),
                    Schema.Field.of("year_of_birth", Schema.FieldType.INT32),
                    Schema.Field.of("month_of_birth", Schema.FieldType.INT32),
                    Schema.Field.of("day_of_birth", Schema.FieldType.INT32),
                    Schema.Field.of("race_concept_id", Schema.FieldType.INT32),
                    Schema.Field.of("ethnicity_concept_id", Schema.FieldType.INT32)
            ).build();
    private final Schema conditionSchema = Schema.builder()
            .addFields(
                    Schema.Field.of("condition_occurrence_id", Schema.FieldType.INT32),
                    Schema.Field.of("person_id", Schema.FieldType.INT32),
                    Schema.Field.of("condition_concept_id", Schema.FieldType.INT32),
                    Schema.Field.of("concept_name", Schema.FieldType.STRING),
                    Schema.Field.of("condition_start_date", Schema.FieldType.DATETIME)
            ).build();
    private final Schema medicationSchema = Schema.builder()
            .addFields(
                    Schema.Field.of("drug_exposure_id", Schema.FieldType.INT32),
                    Schema.Field.of("person_id", Schema.FieldType.INT32),
                    Schema.Field.of("drug_concept_id", Schema.FieldType.INT32),
                    Schema.Field.of("concept_name", Schema.FieldType.STRING),
                    Schema.Field.of("drug_exposure_start_date", Schema.FieldType.DATETIME),
                    Schema.Field.of("drug_exposure_end_date", Schema.FieldType.DATETIME)
            ).build();
    private final Schema procedureSchema = Schema.builder()
            .addFields(
                    Schema.Field.of("procedure_occurrence_id", Schema.FieldType.INT32),
                    Schema.Field.of("person_id", Schema.FieldType.INT32),
                    Schema.Field.of("procedure_concept_id", Schema.FieldType.INT32),
                    Schema.Field.of("concept_name", Schema.FieldType.STRING),
                    Schema.Field.of("procedure_date", Schema.FieldType.DATETIME)
            ).build();
    private final Schema observationSchema = Schema.builder()
            .addFields(
                    Schema.Field.of("measurement_id", Schema.FieldType.INT32),
                    Schema.Field.of("person_id", Schema.FieldType.INT32),
                    Schema.Field.of("measurement_concept_id", Schema.FieldType.INT32),
                    Schema.Field.of("concept_name", Schema.FieldType.STRING),
                    Schema.Field.of("measurement_date", Schema.FieldType.DATETIME),
                    Schema.Field.of("value_as_number", Schema.FieldType.FLOAT.withNullable(true)),
                    Schema.Field.of("value_as_concept_id", Schema.FieldType.STRING.withNullable(true))
            ).build();
}
