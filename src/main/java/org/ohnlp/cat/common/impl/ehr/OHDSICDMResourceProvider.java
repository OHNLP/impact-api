package org.ohnlp.cat.common.impl.ehr;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.hl7.fhir.r4.model.*;
import org.ohnlp.cat.api.criteria.ClinicalEntityType;
import org.ohnlp.cat.api.criteria.EntityValue;
import org.ohnlp.cat.api.criteria.ValueRelationType;
import org.ohnlp.cat.api.ehr.ResourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class OHDSICDMResourceProvider implements ResourceProvider {

    private String cdmSchemaName;
    private Connection conn;

    @Override
    public void init(Map<String, Object> config) {
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
                return "SELECT condition_occurrence_id, " +
                        "person_id, " +
                        "condition_concept_id, " +
                        "condition_start_date FROM " + cdmSchemaName + ".condition_occurrence ";
            case PROCEDURE:
                return "SELECT procedure_occurrence_id, " +
                        "person_id, " +
                        "procedure_concept_id, " +
                        "procedure_date FROM " + cdmSchemaName + ".procedure_occurrence";
            case MEDICATION:
                return "SELECT drug_exposure_id, " +
                        "person_id, " +
                        "drug_concept_id, " +
                        "drug_exposure_start_date," +
                        "drug_exposure_end_date FROM " + cdmSchemaName + ".drug_exposure ";
            case OBSERVATION:
                return "SELECT observation_id, " +
                        "person_id, " +
                        "observation_concept_id, " +
                        "observation_date," +
                        "value_as_number," +
                        "value_as_string FROM " + cdmSchemaName + ".observation";
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
                return "observation_id = ?";
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
                return "observation_id";
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
    public Set<EntityValue> convertToLocalTerminology(ClinicalEntityType type, EntityValue input) {
        // Only process code values
        switch (input.getValuePath()) {
            case CONDITION_CODE:
            case PROCEDURE_CODE:
            case MEDICATION_CODE:
            case OBSERVATION_CODE:
                break;
            default:
                return Collections.singleton(input);
        }
        String[] umlsCodes = input.getValues();
        Set<String> resultCodes = new HashSet<>();
        // Only EQ and IN operations are supported for _CODE paths, so we can just flat expand
        // the resulting values
        try {
            PreparedStatement lookupPS = conn.prepareStatement("SELECT concept_id FROM cdm.CONCEPT WHERE concept_code = ?");
            for (String s : umlsCodes) {
                lookupPS.setString(1, s.toUpperCase(Locale.ROOT));
                ResultSet rs = lookupPS.executeQuery();
                while (rs.next()) {
                    resultCodes.add(rs.getInt("concept_id") + "");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e); // TODO
        }
        if (resultCodes.size() == 0) {
            throw new RuntimeException("Vocab expansion resulted in 0 terms!"); // TODO
        }
        EntityValue val = new EntityValue();
        val.setValuePath(input.getValuePath());
        val.setType(input.getType());
        val.setValues(resultCodes.toArray(new String[0]));
        val.setReln(resultCodes.size() > 1? ValueRelationType.IN : ValueRelationType.EQ);
        return Collections.singleton(val);
    }

    @Override
    public Object[] parseIDTagToParams(ClinicalEntityType type, String evidenceUID) {
        return new Object[] {Long.parseLong(evidenceUID)}; // TODO not all types might be long
    }

    // Row to Resource Mapping functions
    private final SerializableFunction<Row, DomainResource> personMappingFunction = (in) -> {
        String personID = in.getInt64("person_id") + "";
        int genderConceptId = in.getInt32("gender_concept_id");
        int birthyr = in.getInt32("year_of_birth");
        int birthmnth = in.getInt32("month_of_birth");
        int birthday = in.getInt32("day_of_birth");
        int raceConceptId = in.getInt32("race_concept_id");
        int ethnicityConceptId = in.getInt32("ethnicity_concept_id");
        Person p = new Person();
        p.setId(ClinicalEntityType.PERSON + ":" + personID);
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
        String recordID = in.getInt64("condition_occurrence_id") + "";
        String personID = in.getInt64("person_id") + "";
        String conditionConceptID = in.getInt32("condition_concept_id") + "";
        Date dtm = new Date(in.getDateTime("condition_start_date").getMillis());
        Condition cdn = new Condition();
        cdn.setId(ClinicalEntityType.CONDITION + ":" + recordID);
        cdn.setSubject(new Reference().setIdentifier(new Identifier().setValue(personID)));
        cdn.setCode(
                new CodeableConcept().addCoding(
                        new Coding(
                                "https://athena.ohdsi.org/",
                                conditionConceptID,
                                "Autogenerated OHDSI Mapping")) // TODO better mapping/descriptions
        );
        cdn.setRecordedDate(dtm);
        return cdn;
    };

    private final SerializableFunction<Row, DomainResource> medicationMappingFunction = (in) -> {
        String recordID = in.getInt64("drug_exposure_id") + "";
        String personID = in.getInt64("person_id") + "";
        String drugConceptId = in.getInt32("drug_concept_id") + "";
        Date dtm = new Date(in.getDateTime("drug_exposure_start_date").getMillis());
        // TODO see about mapping date ends? there doesn't seem to currently be a target in FHIR somehow (or am just blind)
        MedicationStatement ms = new MedicationStatement();
        ms.setId(ClinicalEntityType.MEDICATION + ":" + recordID);
        ms.setSubject(new Reference().setIdentifier(new Identifier().setValue(personID)));
        ms.setMedication(
                new CodeableConcept().addCoding(
                        new Coding(
                                "https://athena.ohdsi.org/",
                                drugConceptId,
                                "Autogenerated OHDSI Mapping")) // TODO
        );
        ms.setDateAsserted(dtm);
        return ms;
    };

    private final SerializableFunction<Row, DomainResource> procedureMappingFUnction = (in) -> {
        String recordID = in.getInt64("procedure_occurrence_id") + "";
        String personID = in.getInt64("person_id") + "";
        String conceptID = in.getInt32("procedure_concept_id") + "";
        Date dtm = new Date(in.getDateTime("procedure_date").getMillis());
        Procedure prc = new Procedure();
        prc.setId(ClinicalEntityType.PROCEDURE + ":" + recordID);
        prc.setSubject(new Reference().setIdentifier(new Identifier().setValue(personID)));
        prc.setCode(
                new CodeableConcept().addCoding(
                        new Coding(
                                "https://athena.ohdsi.org/",
                                conceptID,
                                "Autogenerated OHDSI Mapping")) // TODO better mapping/descriptions
        );
        prc.setPerformed(new DateTimeType(dtm));
        return prc;
    };

    private final SerializableFunction<Row, DomainResource> observationMappingFunction = (in) -> {
        String recordID = in.getInt64("observation_id") + "";
        String personID = in.getInt64("person_id") + "";
        String conceptID = in.getInt32("observation_concept_id") + "";
        Date dtm = new Date(in.getDateTime("observation_date").getMillis());
        Observation obs = new Observation();
        obs.setId(ClinicalEntityType.OBSERVATION + ":" + recordID);
        obs.setSubject(new Reference().setIdentifier(new Identifier().setValue(personID)));
        obs.setCode(
                new CodeableConcept().addCoding(
                        new Coding(
                                "https://athena.ohdsi.org/",
                                conceptID,
                                "Autogenerated OHDSI Mapping")) // TODO better mapping/descriptions
        );
        String value = null;
        if (in.getFloat("value_as_number") != null) {
            value = in.getFloat("value_as_number") + "";
        } else {
            value = in.getString("value_as_string");
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
                    Schema.Field.of("person_id", Schema.FieldType.INT64),
                    Schema.Field.of("gender_concept_id", Schema.FieldType.INT32),
                    Schema.Field.of("year_of_birth", Schema.FieldType.INT32),
                    Schema.Field.of("month_of_birth", Schema.FieldType.INT32),
                    Schema.Field.of("day_of_birth", Schema.FieldType.INT32),
                    Schema.Field.of("race_concept_id", Schema.FieldType.INT32),
                    Schema.Field.of("ethnicity_concept_id", Schema.FieldType.INT32)
            ).build();
    private final Schema conditionSchema = Schema.builder()
            .addFields(
                    Schema.Field.of("condition_occurrence_id", Schema.FieldType.INT64),
                    Schema.Field.of("person_id", Schema.FieldType.INT64),
                    Schema.Field.of("condition_concept_id", Schema.FieldType.INT32),
                    Schema.Field.of("condition_start_date", Schema.FieldType.DATETIME)
            ).build();
    private final Schema medicationSchema = Schema.builder()
            .addFields(
                    Schema.Field.of("drug_exposure_id", Schema.FieldType.INT64),
                    Schema.Field.of("person_id", Schema.FieldType.INT64),
                    Schema.Field.of("drug_concept_id", Schema.FieldType.INT32),
                    Schema.Field.of("drug_exposure_start_date", Schema.FieldType.DATETIME),
                    Schema.Field.of("drug_exposure_end_date", Schema.FieldType.DATETIME)
            ).build();
    private final Schema procedureSchema = Schema.builder()
            .addFields(
                    Schema.Field.of("procedure_occurrence_id", Schema.FieldType.INT64),
                    Schema.Field.of("person_id", Schema.FieldType.INT64),
                    Schema.Field.of("procedure_concept_id", Schema.FieldType.INT32),
                    Schema.Field.of("procedure_date", Schema.FieldType.DATETIME)
            ).build();
    private final Schema observationSchema = Schema.builder()
            .addFields(
                    Schema.Field.of("observation_id", Schema.FieldType.INT64),
                    Schema.Field.of("person_id", Schema.FieldType.INT64),
                    Schema.Field.of("observation_concept_id", Schema.FieldType.INT32),
                    Schema.Field.of("observation_date", Schema.FieldType.DATETIME),
                    Schema.Field.of("value_as_number", Schema.FieldType.FLOAT),
                    Schema.Field.of("value_as_string", Schema.FieldType.STRING)
            ).build();
}
