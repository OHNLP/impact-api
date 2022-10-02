package org.ohnlp.cat.api.criteria;

import java.io.Serializable;

public enum LogicalRelationType implements Serializable {
    AND,
    MIN_OR,
    MAX_OR,
    NOT
}
