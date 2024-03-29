package org.ohnlp.cat.api.criteria;

import java.io.Serializable;

public enum FHIRValueLocationPath implements Serializable {
    PERSON_ID("id"),
    PERSON_GENDER("gender"),
    PERSON_DOB("birthDate"),
    CONDITION_CODE("code.coding.code", true),
    PROCEDURE_CODE("code.coding.code", true),
    MEDICATION_CODE("medicationCodeableConcept.coding.code", true),
    OBSERVATION_CODE("code.coding.code", true),
    OBSERVATION_VALUE("valueString");

    private final String path;
    private final boolean coded;

    FHIRValueLocationPath(String path) {
        this(path, false);
    }

    FHIRValueLocationPath(String path, boolean coded) {
        this.coded = coded;
        this.path = path;
    }

    public boolean isCoded() {
        return coded;
    }

    public String getPath() {
        return path;
    }
}
