package org.ohnlp.cat.common.impl.sql;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.util.FhirTerser;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.hl7.fhir.r4.model.*;
import org.ohnlp.cat.api.criteria.ClinicalEntityType;
import org.ohnlp.cat.api.criteria.FHIRValueLocationPath;
import org.ohnlp.cat.api.ehr.ResourceProvider;
import org.ohnlp.cat.api.utils.FHIRUtils;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class GenericSQLviaJDBCResourceProvider implements ResourceProvider {

    private Map<ClinicalEntityType, String> queries;
    private Map<ClinicalEntityType, Schema> schemas;
    private Map<ClinicalEntityType, SerializableFunction<Row, DomainResource>> mappingFunctions;
    private Map<ClinicalEntityType, String[]> idCols;
    private String sourceName;


    @Override
    public void init(String sourceName, Map<String, Object> config) {
        this.sourceName = sourceName;
        this.queries = new HashMap<>();
        this.schemas = new HashMap<>();
        this.mappingFunctions = new HashMap<>();
        for (ClinicalEntityType type : ClinicalEntityType.values()) {
            Map<String, Object> base = (Map<String, Object>) config.get(type.name().toLowerCase(Locale.ROOT));
            if (base != null) {
                String[] idCols = null;
                queries.put(type, base.get("query").toString());
                Schema.Builder builder = Schema.builder();
                ((Map<String, Object>) base.get("schema"))
                        .forEach((col, field) ->
                                builder.addField(col, Schema.FieldType.of(Schema.TypeName.valueOf(field.toString()))));
                schemas.put(type, builder.build());
                Map<String, String> fhirPathMappings = new HashMap<>();
                for (Map.Entry<String, Object> e : ((Map<String, Object>) base.get("mappings")).entrySet()) {
                    String col = e.getKey();
                    String target = e.getValue().toString();
                    fhirPathMappings.put(col, target.toString());
                    if (target.toString().equalsIgnoreCase("ID")) {
                        idCols = col.split(",");
                    }
                }
                if (idCols == null) {
                    throw new IllegalArgumentException("No ID Mapping/Columns Provided");
                }
                this.mappingFunctions.put(type, new GenericPathBasedMappingFunction(type, fhirPathMappings));
            }

        }
    }

    @Override
    public String getQuery(ClinicalEntityType type) {
        return queries.get(type);
    }

    @Override
    public Schema getQuerySchema(ClinicalEntityType type) {
        return schemas.get(type);
    }

    @Override
    public String getEvidenceIDFilter(ClinicalEntityType type) {
        if (idCols.get(type) != null) {
            return Arrays.stream(idCols.get(type)).map(s -> s + " = ?").collect(Collectors.joining(" AND "));
        }
        return null;
    }

    @Override
    public String getIndexableIDColumnName(ClinicalEntityType type) {
        return Arrays.stream(idCols.get(type)).collect(Collectors.joining(","));
    } // TODO investigate where this is actually used to make sure it works for multi-column IDs

    @Override
    public SerializableFunction<Row, DomainResource> getRowToResourceMapper(ClinicalEntityType type) {
        return mappingFunctions.get(type);
    }

    @Override
    public Set<String> convertToLocalTerminology(ClinicalEntityType type, String input) {
        return null; // TODO re-implement with #6
    }

    @Override
    public Object[] parseIDTagToParams(ClinicalEntityType type, String evidenceUID) {
        String[] cols = idCols.get(type);
        if (cols == null) {
            return null;
        }
        String[] rawUID = evidenceUID.split("\\|~\\|");
        if (cols.length != rawUID.length) {
            throw new IllegalArgumentException("ID Mapping Column Count does not match Supplied Evidence UID! Expected " + String.join(",", cols) + " Got: " + evidenceUID);
        }
        Object[] ret = new Object[rawUID.length];
        // Get types
        for (int i = 0; i < cols.length; i++) {
            Schema.FieldType fieldType = schemas.get(type).getField(cols[i]).getType();
            switch (fieldType.getTypeName()) {
                case BYTE:
                    ret[i] = Byte.valueOf(rawUID[i]);
                    break;
                case INT16:
                    ret[i] = Short.valueOf(rawUID[i]);
                    break;
                case INT32:
                    ret[i] = Integer.valueOf(rawUID[i]);
                    break;
                case INT64:
                    ret[i] = Long.valueOf(rawUID[i]);
                    break;
                case DECIMAL:
                case FLOAT:
                    ret[i] = Float.parseFloat(rawUID[i]);
                    break;
                case DOUBLE:
                    ret[i] = Double.parseDouble(rawUID[i]);
                    break;
                case STRING:
                    ret[i] = rawUID[i];
                    break;
                case DATETIME:
                    ret[i] = Instant.parse(rawUID[i]);
                    break;
                case BOOLEAN:
                    ret[i] = Boolean.valueOf(rawUID[i]);
                    break;
                case BYTES:
                case ARRAY:
                    case ITERABLE:
                case MAP:
                case ROW:
                case LOGICAL_TYPE:
                    throw new UnsupportedOperationException("Unsupported type used in ID definition: " + fieldType.getTypeName());
            }
        }
        return ret;
    }

    @Override
    public String getPathForValueReference(FHIRValueLocationPath valueRef) {
        return valueRef.getPath();
    }

    @Override
    public String extractPatUIDForResource(ClinicalEntityType type, DomainResource r) { // TODO verify functionality
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

    public static class GenericPathBasedMappingFunction implements SerializableFunction<Row, DomainResource> {

        private final Map<String, String> mappings;
        private transient ThreadLocal<FhirTerser> terser;
        private final ClinicalEntityType type;

        public GenericPathBasedMappingFunction(ClinicalEntityType type, Map<String, String> fhirPathMappings) {
            this.type = type;
            this.mappings = fhirPathMappings;
        }

        @Override
        public DomainResource apply(Row input) {
            Map<String, Collection<String>> pathToValue = new HashMap<>();
            AtomicReference<String> id = new AtomicReference<>();
            this.mappings.forEach((colNames, target) -> {
                String[] cols = colNames.split(",");
                List<String> values = new ArrayList<>();
                for (String col : cols) {
                    values.add(input.getValue(col).toString()); // TODO date type coercion might be broken here
                }
                String value = String.join("|~|", values);
                if (target.equalsIgnoreCase("ID")) {
                    id.set(value);
                } else {
                    pathToValue.computeIfAbsent(target, k -> new HashSet<>()).add(value);
                }
            });
            if (id.get() == null) {
                throw new IllegalStateException("No ID mapping is provided");
            }
            if (terser == null) {
                terser = ThreadLocal.withInitial(() -> FhirContext.forR4().newTerser());
            }
            return FHIRUtils.createAndWriteValuestoResource(terser.get(), type, id.get(), pathToValue);
        }
    }
}
