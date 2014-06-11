package pt.owlsql.extractors;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLAnnotation;
import org.semanticweb.owlapi.model.OWLAnnotationProperty;
import org.semanticweb.owlapi.model.OWLAnnotationValue;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLEntity;
import org.semanticweb.owlapi.model.OWLLiteral;
import org.semanticweb.owlapi.model.OWLOntology;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

import pt.json.JSONException;
import pt.owlsql.OWLExtractor;


public final class NamesExtractor extends OWLExtractor {
    
    private final SQLCoreUtils utils = getExtractor(SQLCoreUtils.class);
    
    private final ArrayList<OWLAnnotationProperty> properties = new ArrayList<>();
    
    private PreparedStatement getAllNames;
    private PreparedStatement getAllNamesOnProperty;
    private PreparedStatement getOneName;
    private PreparedStatement getOneNameOnProperty;
    
    
    @Override
    protected void extract(Set<OWLOntology> ontologies) throws SQLException {
        if (properties.size() == 0)
            properties.add(factory.getRDFSLabel());
        
        @SuppressWarnings("resource")
        Statement statement = getConnection().createStatement();
        
        statement.execute("DROP TABLE IF EXISTS names");
        statement.execute("CREATE TABLE names ("
                + "id INT, "
                + "property INT, "
                + "priority INT, "
                + "name TEXT, "
                + "INDEX (id), "
                + "INDEX (name(64)), "
                + "UNIQUE (id, priority))");
        statement.close();
        
        @SuppressWarnings("resource")
        PreparedStatement getPriority = getConnection()
                .prepareStatement("SELECT MAX(priority) FROM names WHERE id = ?");
        @SuppressWarnings("resource")
        PreparedStatement insertName = getConnection()
                .prepareStatement("INSERT INTO names (id, property, priority, name) VALUES (?, ?, ?, ?)");
        
        int counter = 0;
        for (OWLOntology ontology : ontologies) {
            for (OWLEntity owlEntity : ontology.getSignature(true)) {
                int id = utils.getID(owlEntity);
                for (OWLAnnotationProperty property : properties) {
                    int propertyID = utils.getID(property);
                    Set<OWLAnnotation> annotations = owlEntity.getAnnotations(ontology, property);
                    for (OWLOntology closure : ontology.getImportsClosure()) {
                        annotations.addAll(owlEntity.getAnnotations(closure, property));
                    }
                    for (OWLAnnotation annotation : annotations) {
                        OWLAnnotationValue value = annotation.getValue();
                        if (value instanceof OWLLiteral) {
                            OWLLiteral literal = (OWLLiteral) value;
                            String name = literal.getLiteral();
                            
                            int priority;
                            getPriority.setInt(1, id);
                            try (ResultSet resultSet = getPriority.executeQuery()) {
                                resultSet.next();
                                priority = resultSet.getInt(1) + 1;
                            }
                            
                            insertName.setInt(1, id);
                            insertName.setInt(2, propertyID);
                            insertName.setInt(3, priority);
                            insertName.setString(4, name);
                            insertName.executeUpdate();
                            
                            counter++;
                            if (counter % 1000 == 0) {
                                System.out.println("... " + counter + " names found ...");
                            }
                        }
                    }
                }
            }
        }
        
        getPriority.close();
        insertName.close();
    }
    
    
    @Override
    protected String[] getMandatoryOptions() {
        return new String[] { "properties" };
    }
    
    
    @Override
    protected void prepare() throws SQLException {
        Connection connection = getConnection();
        
        getAllNames = connection.prepareStatement("SELECT name FROM names WHERE id = ?");
        getAllNamesOnProperty = connection.prepareStatement(""
                + "SELECT name "
                + "FROM names "
                + "WHERE id = ? AND property = ?");
        getOneName = connection.prepareStatement(""
                + "SELECT name "
                + "FROM names "
                + "WHERE id = ? "
                + "ORDER BY priority LIMIT 1");
        getOneNameOnProperty = connection.prepareStatement(""
                + "SELECT name "
                + "FROM names "
                + "WHERE id = ? AND property = ? "
                + "ORDER BY priority LIMIT 1");
    }
    
    
    @Override
    protected void processOption(String key, JsonElement element) throws JSONException {
        if (key.equals("properties")) {
            if (!element.isJsonArray())
                throw new JSONException("must be a list");
            JsonArray array = element.getAsJsonArray();
            for (int i = 0; i < array.size(); i++) {
                JsonElement inner = array.get(i);
                if (!inner.isJsonPrimitive() || !inner.getAsJsonPrimitive().isString())
                    throw new JSONException("must be a string", "[" + i + "]");
                String string = inner.getAsString();
                properties.add(factory.getOWLAnnotationProperty(IRI.create(string)));
            }
        }
        else {
            super.processOption(key, element);
        }
    }
    
    
    public HashSet<String> getAllNames(OWLClass owlClass) throws SQLException {
        HashSet<String> result = new HashSet<>();
        
        getAllNames.setInt(1, utils.getID(owlClass));
        try (ResultSet resultSet = getAllNames.executeQuery()) {
            while (resultSet.next()) {
                result.add(resultSet.getString(1));
            }
        }
        return result;
    }
    
    
    public HashSet<String> getAllNamesOnProperty(OWLClass owlClass, OWLAnnotationProperty property) throws SQLException {
        HashSet<String> result = new HashSet<>();
        
        getAllNamesOnProperty.setInt(1, utils.getID(owlClass));
        getAllNamesOnProperty.setInt(2, utils.getID(property));
        try (ResultSet resultSet = getAllNamesOnProperty.executeQuery()) {
            while (resultSet.next()) {
                result.add(resultSet.getString(1));
            }
        }
        return result;
    }
    
    
    public String getMainName(OWLClass owlClass) throws SQLException {
        getOneName.setInt(1, utils.getID(owlClass));
        try (ResultSet resultSet = getOneName.executeQuery()) {
            if (resultSet.next())
                return resultSet.getString(1);
        }
        return null;
    }
    
    
    public String getMainNameOnProperty(OWLClass owlClass, OWLAnnotationProperty property) throws SQLException {
        getOneNameOnProperty.setInt(1, utils.getID(owlClass));
        getOneNameOnProperty.setInt(2, utils.getID(property));
        try (ResultSet resultSet = getOneName.executeQuery()) {
            if (resultSet.next())
                return resultSet.getString(1);
        }
        return null;
    }
}
