package org.ohnlp.cat.common.impl.criteria.representations;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.ohnlp.cat.api.criteria.ClinicalEntityType;
import org.ohnlp.cat.api.criteria.parser.DataSourceRepresentation;
import org.ohnlp.cat.api.criteria.parser.UMLSDataSourceRepresentationResolver;

import java.sql.*;
import java.util.*;

public class UMLSSourceVocabJDBCResolver implements UMLSDataSourceRepresentationResolver {

    Map<ClinicalEntityType, List<String>> sourceVocabMapping;
    private String jdbcurl;
    private String tableName;

    @Override
    public void init(Map<String, Object> config) {
        this.sourceVocabMapping = new HashMap<>();
        JsonNode json = new ObjectMapper().valueToTree(config);
        String driverClass = json.get("driver").asText();
        this.jdbcurl = json.get("jdbcURL").asText();
        this.tableName = json.get("mrconsotablename").asText();
        JsonNode sourceVocabs = json.get("vocabs");
        for (ClinicalEntityType type : ClinicalEntityType.values()) {
            if (sourceVocabs.has(type.name())) {
                List<String> srcVocabs = new ArrayList<>();
                for (JsonNode src : ((ArrayNode)sourceVocabs.get(type.name()))) {
                    srcVocabs.add(src.asText());
                }
                sourceVocabMapping.put(type, srcVocabs);
            }
        }

        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
           throw new RuntimeException(e);
        }
    }

    @Override
    public Set<DataSourceRepresentation> resolveForUMLS(ClinicalEntityType type, String requestingDataSourceID, String umlsCUI) {
        HashSet<DataSourceRepresentation> ret = new HashSet<>();
        List<String> vocabs = sourceVocabMapping.get(type);
        if (vocabs != null) {
            //TODO handle authentication etc
            try (Connection conn = DriverManager.getConnection(jdbcurl)){
                PreparedStatement ps = conn.prepareStatement("SELECT CODE, STR FROM " + this.tableName + " WHERE CUI = ? AND SAB = ?");
                ps.setString(1, umlsCUI);
                for (String vocab : vocabs) {
                    ps.setString(2, vocab);
                    ResultSet rs = ps.executeQuery();
                    while (rs.next()) {
                        String code = rs.getString(1);
                        String str = rs.getString(2);
                        DataSourceRepresentation representation = new DataSourceRepresentation();
                        representation.setRepresentation(code);
                        representation.setRepresentationDescription(str);
                        representation.setResolverID(vocab);
                        ret.add(representation);
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return ret;
    }
}
