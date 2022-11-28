package org.ohnlp.cat.common.impl.criteria.representations;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.ohnlp.cat.api.criteria.ClinicalEntityType;
import org.ohnlp.cat.api.criteria.parser.DataSourceRepresentation;
import org.ohnlp.cat.api.criteria.parser.UMLSDataSourceRepresentationResolver;

import java.sql.*;
import java.util.*;

public class UMLSCustomMappingJDBCResolver implements UMLSDataSourceRepresentationResolver {

    private String jdbcurl;
    private String tableName;
    private String resolverId;

    @Override
    public void init(Map<String, Object> config) {
        JsonNode json = new ObjectMapper().valueToTree(config);
        this.resolverId = json.get("resolverID").asText();
        String driverClass = json.get("driver").asText();
        this.jdbcurl = json.get("jdbcURL").asText();
        this.tableName = json.get("tableName").asText();


        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<DataSourceRepresentation> resolveForUMLS(ClinicalEntityType type, Collection<String> requestingDataSourceIDs, String umlsCUI) {
        HashSet<DataSourceRepresentation> ret = new HashSet<>();
        //TODO handle authentication etc
        try (Connection conn = DriverManager.getConnection(jdbcurl)) {
            PreparedStatement ps = conn.prepareStatement("SELECT CODE, CODE_DESC FROM " + this.tableName + " WHERE CUI = ?");
            ps.setString(1, umlsCUI);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String code = rs.getString(1);
                String str = rs.getString(2);
                DataSourceRepresentation representation = new DataSourceRepresentation();
                representation.setRepresentation(code);
                representation.setRepresentationDescription(str);
                representation.setResolverID(resolverId);
                representation.setDataSourceID(requestingDataSourceIDs);
                ret.add(representation);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return ret;
    }
}
