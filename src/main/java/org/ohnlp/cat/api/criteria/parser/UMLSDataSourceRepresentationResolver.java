package org.ohnlp.cat.api.criteria.parser;

import org.ohnlp.cat.api.criteria.ClinicalEntityType;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public interface UMLSDataSourceRepresentationResolver {
    void init(Map<String, Object> config);
    Set<DataSourceRepresentation> resolveForUMLS(ClinicalEntityType type, Collection<String> requestingDataSourceIDs, String umlsCUI);

}
