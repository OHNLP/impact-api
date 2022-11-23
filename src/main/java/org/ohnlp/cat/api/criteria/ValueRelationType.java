package org.ohnlp.cat.api.criteria;

import java.io.Serializable;

public enum ValueRelationType implements Serializable {
    LT,
    LTE,
    GT,
    GTE,
    EQ,
    RANGE,
    IN,
    BETWEEN
}
