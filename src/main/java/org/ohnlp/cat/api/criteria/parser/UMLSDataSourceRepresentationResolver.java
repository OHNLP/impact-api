package org.ohnlp.cat.api.criteria.parser;

import java.util.Map;
import java.util.Set;

public interface UMLSDataSourceRepresentationResolver {
    void init(Map<String, Object> config);
    Set<DataSourceRepresentation> resolveForUMLS(String umlsCUI);

}
