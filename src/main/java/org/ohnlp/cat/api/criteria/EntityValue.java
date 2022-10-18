package org.ohnlp.cat.api.criteria;

import ca.uhn.fhir.context.FhirContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.hl7.fhir.r4.model.DomainResource;
import org.ohnlp.cat.api.ehr.ResourceProvider;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EntityValue implements Serializable {
    private FHIRValueLocationPath valuePath;
    private String[] values;
    private ValueRelationType reln;
    private String[][] expandedCodes;
    // Transient Variables used for value matching
    private transient FhirContext internalContext;
    private transient ThreadLocal<SimpleDateFormat> sdf;
    private transient ThreadLocal<ObjectMapper> om;

    // Logic used to evaluate whether a given value definition matches the provided domain resource
    public boolean matches(DomainResource resource, ResourceProvider provider) {
        if (internalContext == null) {
            internalContext = FhirContext.forR4();
        }
        if (sdf == null) {
            sdf = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd"));
        }
        if (om == null) {
            om = ThreadLocal.withInitial(ObjectMapper::new);
        }
        String resourceJSON = internalContext.newJsonParser().encodeResourceToString(resource);
        LinkedList<String> pathStack = new LinkedList<>(Arrays.asList(provider.getPathForValueReference(valuePath).split("\\.")));
        List<String> valueList = new ArrayList<>();
        try {
            JsonNode json = om.get().readTree(resourceJSON);
            findValuesFromJsonPath(json, pathStack, valueList);
        } catch (JsonProcessingException e) {
            return false;
        }
        for (String value : valueList) {
            if (coerceAndCompare(value)) {
                return true;
            }
        }
        return false;
    }

    private void findValuesFromJsonPath(JsonNode json, LinkedList<String> pathStack, List<String> valueList) {
        // Just simply always iterate if array
        if (json instanceof ArrayNode) {
            for (JsonNode child : json) {
                findValuesFromJsonPath(child, pathStack, valueList);
            }
            return;
        }
        if (pathStack.size() > 0) {
            String first = pathStack.removeFirst();
            if (json.has(first)) {
                findValuesFromJsonPath(json.get(first), pathStack, valueList);
            }
        } else {
            // Return current JSON value
            valueList.add(json.asText());
        }
    }

    private boolean coerceAndCompare(String value) {
        try {
            return compareNumeric(Double.parseDouble(value));
        } catch (NumberFormatException ignored) {
        }
        // Next try date format, yyyy-MM-dd
        try {
            return compareDates(sdf.get().parse(value));
        } catch (ParseException ignored) {
        }
        // Finally, do a direct string compare.
        // Here, we need to check if this is a coded/valueset-based comparison first, as in such cases
        // we ignore values and go directly for expandedCodes
        if (valuePath.isCoded()) {
            if (reln.equals(ValueRelationType.IN)) {
                return Arrays.stream(expandedCodes).flatMap(Arrays::stream).map(String::toLowerCase).collect(Collectors.toSet()).contains(value.toLowerCase(Locale.ROOT));
            } else if (reln.equals(ValueRelationType.EQ)) {
                return Arrays.stream(expandedCodes[0]).map(String::toLowerCase).collect(Collectors.toSet()).contains(value.toLowerCase(Locale.ROOT));
            } else {
                throw new UnsupportedOperationException("Cannot execute " + reln + " on undefined/string datatype");
            }
        } else {
            if (reln.equals(ValueRelationType.IN)) {
                return Arrays.stream(values).map(String::toLowerCase).collect(Collectors.toSet()).contains(value.toLowerCase(Locale.ROOT));
            } else if (reln.equals(ValueRelationType.EQ)) {
                return values[0].equalsIgnoreCase(value.toLowerCase(Locale.ROOT));
            } else {
                throw new UnsupportedOperationException("Cannot execute " + reln + " on undefined/string datatype");
            }
        }
    }


    private boolean compareNumeric(double input) throws NumberFormatException {
        double val1 = Double.parseDouble(values[0]);
        switch (reln) {
            case LT:
                return input < val1;
            case LTE:
                return input <= val1;
            case GT:
                return input > val1;
            case GTE:
                return input >= val1;
            case EQ:
                return input == val1;
            case BETWEEN:
                double val2 = Double.parseDouble(values[1]);
                return input >= val1 && input < val2;
            case IN:
                Set<Double> valueset = new HashSet<>();
                Arrays.stream(values).mapToDouble(Double::parseDouble).forEach(valueset::add);
                return valueset.contains(input);
        }
        return false;
    }

    private boolean compareDates(Date input) throws ParseException {
        Date val1 = sdf.get().parse(values[0]);
        switch (reln) {
            case LT:
                return input.getTime() < val1.getTime();
            case LTE:
                return input.getTime() <= val1.getTime();
            case GT:
                return input.getTime() > val1.getTime();
            case GTE:
                return input.getTime() >= val1.getTime();
            case EQ:
                return input.getTime() == val1.getTime();
            case BETWEEN:
                Date val2 = sdf.get().parse(values[1]);
                return input.getTime() >= val1.getTime() && input.getTime() < val2.getTime();
            case IN:
                Set<Long> valueset = new HashSet<>();
                Arrays.stream(values).flatMap(s -> {
                    try {
                        return Stream.of(sdf.get().parse(s).getTime());
                    } catch (ParseException e) {
                        return Stream.empty();
                    }
                }).forEach(valueset::add);
                return valueset.contains(input.getTime());
        }
        return false;
    }

    // POJO methods
    public FHIRValueLocationPath getValuePath() {
        return valuePath;
    }

    public void setValuePath(FHIRValueLocationPath valuePath) {
        this.valuePath = valuePath;
    }

    public String[] getValues() {
        return values;
    }

    public void setValues(String[] values) {
        this.values = values;
    }

    public ValueRelationType getReln() {
        return reln;
    }

    public void setReln(ValueRelationType reln) {
        this.reln = reln;
    }

    public String[][] getExpandedCodes() {
        return expandedCodes;
    }

    public void setExpandedCodes(String[][] expandedCodes) {
        this.expandedCodes = expandedCodes;
    }
}
