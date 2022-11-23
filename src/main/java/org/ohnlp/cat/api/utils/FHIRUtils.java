package org.ohnlp.cat.api.utils;

import ca.uhn.fhir.util.FhirTerser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.hl7.fhir.r4.model.*;
import org.ohnlp.cat.api.criteria.ClinicalEntityType;

import java.util.*;

public class FHIRUtils {

    public static LinkedList<String> parsePath(String path) {
        return new LinkedList<>(Arrays.asList(path.split("\\.")));
    }

    public static List<String> findValuesFromJsonPath(JsonNode json, String path) {
        List<String> ret = new ArrayList<>();
        LinkedList<String> pathStack = parsePath(path);
        findValuesFromJsonPathRecurs(json, pathStack, ret);
        return ret;
    }

    private static void findValuesFromJsonPathRecurs(JsonNode json, LinkedList<String> pathStack, List<String> valueList) {
        // Just simply always iterate if array
        if (json instanceof ArrayNode) {
            for (JsonNode child : json) {
                findValuesFromJsonPathRecurs(child, pathStack, valueList);
            }
            return;
        }

        if (pathStack.size() > 0) {
            String first = pathStack.removeFirst();
            if (json.has(first)) {
                findValuesFromJsonPathRecurs(json.get(first), pathStack, valueList);
            }
        } else {
            // Return current JSON value
            valueList.add(json.asText());
        }
    }

    public static DomainResource createAndWriteValuestoResource(FhirTerser terser, ClinicalEntityType type, String id, Map<String, Collection<String>> values) {
        DomainResource root;
        switch (type) {
            case PERSON:
                root = new Person();
                break;
            case CONDITION:
                root = new Condition();
                break;
            case PROCEDURE:
                root = new Procedure();
                break;
            case MEDICATION:
                root = new MedicationStatement();
                break;
            case OBSERVATION:
                root = new Observation();
                break;
            default:
                throw new IllegalArgumentException("Unsupported type " + type);
        }
        root.setId(id);
        values.forEach((path, value) -> terser.addElements(root, path, value));
        return root;
    }
}
