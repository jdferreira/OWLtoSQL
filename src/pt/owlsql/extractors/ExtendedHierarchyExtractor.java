package pt.owlsql.extractors;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Set;

import org.semanticweb.owlapi.model.AxiomType;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLObjectPropertyExpression;
import org.semanticweb.owlapi.model.OWLObjectSomeValuesFrom;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom;

import pt.json.JSONException;
import pt.owlsql.OWLExtractor;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;


public final class ExtendedHierarchyExtractor extends OWLExtractor {
    
    private PreparedStatement getDepthStatement;
    
    private PreparedStatement getMaxDepthStatement;
    private PreparedStatement selectAncestrySizeStatement;
    private PreparedStatement selectAncestryStatement;
    private PreparedStatement selectDescendantsSizeStatement;
    private PreparedStatement selectDescendantsStatement;
    private final SQLCoreUtils utils = getExtractor(SQLCoreUtils.class);
    
    
    private final HashSet<OWLObjectProperty> properties = new HashSet<>();
    private String identifier;
    private boolean emulateReflexive;
    private boolean emulateNotReflexive;
    private boolean emulateTransitive;
    private boolean emulateNotTransitive;
    
    private boolean transitive = false;
    private boolean reflexive = false;
    private boolean subProperties = true;
    
    
    private void getSubProperties(Set<OWLOntology> ontologies) {
        if (!subProperties)
            return;
        
        System.out.println("finding the subproperties of ");
        for (OWLObjectProperty property : properties) {
            System.out.println("- " + property.toStringID());
        }
        
        // Store the actual properties given by the user
        ArrayDeque<OWLObjectProperty> toAdd = new ArrayDeque<>(properties);
        
        // Clear the current properties (will add them iteratively alter
        properties.clear();
        
        while (toAdd.size() > 0) {
            OWLObjectProperty property = toAdd.pop();
            if (properties.contains(property))
                continue;
            properties.add(property);
            System.out.println(">>> " + property.toStringID());
            
            for (OWLObjectPropertyExpression propertyExpression : property.getSubProperties(ontologies)) {
                if (!propertyExpression.isAnonymous())
                    toAdd.add(propertyExpression.asOWLObjectProperty());
            }
        }
    }
    
    
    private void setPropertyProperties(Set<OWLOntology> ontologies) {
        if (emulateReflexive)
            reflexive = true;
        else if (emulateNotReflexive)
            reflexive = false;
        else {
            reflexive = true;
            for (OWLObjectProperty property : properties) {
                if (!property.isReflexive(ontologies)) {
                    reflexive = false;
                    break;
                }
            }
        }
        
        if (emulateTransitive)
            transitive = true;
        else if (emulateNotTransitive)
            transitive = false;
        else {
            for (OWLObjectProperty property : properties) {
                if (!property.isTransitive(ontologies)) {
                    reflexive = false;
                    break;
                }
            }
        }
    }
    
    
    @Override
    protected void extract(Set<OWLOntology> ontologies) throws SQLException {
        Connection connection = getConnection();
        
        try (Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS extended_hierarchy");
            statement.execute("CREATE TABLE extended_hierarchy ("
                    + "  extension VARCHAR(256),"
                    + "  subclass INT,"
                    + "  superclass INT,"
                    + "  distance INT,"
                    + "  INDEX (extension),"
                    + "  INDEX (subclass),"
                    + "  INDEX (superclass),"
                    + "  INDEX (distance),"
                    + "  UNIQUE (extension, subclass, superclass))");
        }
        
        setPropertyProperties(ontologies);
        getSubProperties(ontologies);
        
        // Let's insert the direct relations on the table
        // If Subclass(A ObjectSomeValuesFrom(P B)):
        // __ If A == B:
        // __ __ insert (A, B, 0)
        // __ Else:
        // __ __ insert (A, B, 1)
        try (PreparedStatement stmt = connection.prepareStatement(""
                + "INSERT IGNORE INTO extended_hierarchy (extension, subclass, superclass, distance) "
                + "VALUES (?, ?, ?, ?)")) {
            stmt.setString(1, identifier);
            
            int counter = 0;
            
            for (OWLOntology ontology : ontologies) {
                for (OWLSubClassOfAxiom axiom : ontology.getAxioms(AxiomType.SUBCLASS_OF)) {
                    if (axiom.getSubClass().isAnonymous())
                        continue;
                    if (axiom.getSuperClass() instanceof OWLObjectSomeValuesFrom) {
                        OWLObjectSomeValuesFrom restriction = (OWLObjectSomeValuesFrom) axiom.getSuperClass();
                        if (properties.contains(restriction.getProperty())
                                && restriction.getFiller() instanceof OWLClass) {
                            int id1 = utils.getID((OWLClass) axiom.getSubClass());
                            int id2 = utils.getID((OWLClass) restriction.getFiller());
                            int distance = id1 == id2 ? 0 : 1;
                            stmt.setInt(2, id1);
                            stmt.setInt(3, id2);
                            stmt.setInt(4, distance);
                            stmt.execute();
                            
                            counter++;
                            if (counter % 1000 == 0) {
                                System.out.println("... " + counter + " relations found ...");
                            }
                        }
                    }
                }
            }
            
            System.out.println("... " + counter + " direct relations found ...");
            stmt.executeBatch();
        }
        
        if (transitive) {
            System.out.println("... closing the graph because this is a transitive relation");
            try (PreparedStatement stmt = connection.prepareStatement(""
                    + "INSERT IGNORE INTO extended_hierarchy (extension, subclass, superclass, distance) "
                    + "SELECT ?, e1.subclass, e2.superclass, e1.distance + 1 "
                    + "FROM extended_hierarchy AS e1, extended_hierarchy AS e2 "
                    + "WHERE e1.extension = ? AND e2.extension = ? AND "
                    + "      e1.superclass = e2.subclass AND"
                    + "      e1.distance = ? AND"
                    + "      e2.distance = 1")) {
                stmt.setString(1, identifier);
                stmt.setString(2, identifier);
                stmt.setString(3, identifier);
                
                int distance = 1;
                while (true) {
                    stmt.setInt(4, distance);
                    int newRows = stmt.executeUpdate();
                    System.out.println("  " + newRows + " relations with distance = " + (distance + 1));
                    if (newRows == 0)
                        break;
                    distance++;
                }
            }
        }
        
        
        if (reflexive) {
            System.out.println("... inserting reflexive relations because this is a reflexive relation");
            try (PreparedStatement stmt = connection.prepareStatement(""
                    + "INSERT INTO extended_hierarchy (extension, subclass, superclass, distance) "
                    + "SELECT ?, id, id, 0 "
                    + "FROM owl_objects "
                    + "WHERE type = 'Class' "
                    + "ON DUPLICATE KEY UPDATE distance = 0")) {
                stmt.setString(1, identifier);
                stmt.executeUpdate();
            }
        }
        
        
        System.out.println("... closing everything based on class-subclass relations");
        try (PreparedStatement stmt = connection.prepareStatement(""
                + "INSERT INTO extended_hierarchy (extension, subclass, superclass, distance) "
                + "SELECT ?, h1.subclass, h2.superclass, h1.distance + h2.distance + e.distance "
                + "FROM hierarchy AS h1, "
                + "     hierarchy AS h2, "
                + "     extended_hierarchy AS e "
                + "WHERE h1.superclass = e.subclass AND "
                + "      h2.subclass = e.superclass AND "
                + "      e.extension = ? AND"
                + "      h1.distance = ? "
                + "ON DUPLICATE KEY UPDATE distance = LEAST(extended_hierarchy.distance, VALUES(distance))")) {
            stmt.setString(1, identifier);
            stmt.setString(2, identifier);
            
            // Add based on different distances in h1. This is a way to divide the huge insert statement into smaller
            // chunks, and allows a certain amount of visualization either from the output or actually from the state
            // of the database
            for (int distance = 0; /* Stop condition is inside the body */; distance++) {
                stmt.setInt(3, distance);
                int inserted = stmt.executeUpdate();
                System.out.println("... inserted " + inserted + " pairs");
                if (inserted == 0)
                    break;
            }
        }
    }
    
    
    @Override
    protected String[] getMandatoryOptions() {
        return new String[] { "properties", "identifier" };
    }
    
    
    @Override
    protected void prepare() throws SQLException {
        Connection connection = getConnection();
        
        selectAncestryStatement = connection.prepareStatement(""
                + "SELECT o_superclass.iri "
                + "FROM extended_hierarchy "
                + "JOIN owl_objects AS o_subclass   ON o_subclass.id   = extended_hierarchy.subclass "
                + "JOIN owl_objects AS o_superclass ON o_superclass.id = extended_hierarchy.superclass "
                + "WHERE extension = ? AND o_subclass.id = ?");
        selectAncestryStatement.setString(1, identifier);
        
        selectAncestrySizeStatement = connection.prepareStatement(""
                + "SELECT COUNT(*) "
                + "FROM extended_hierarchy "
                + "JOIN owl_objects AS o_subclass   ON o_subclass.id   = extended_hierarchy.subclass "
                + "JOIN owl_objects AS o_superclass ON o_superclass.id = extended_hierarchy.superclass "
                + "WHERE extension = ? AND o_subclass.id = ?");
        selectAncestrySizeStatement.setString(1, identifier);
        
        selectDescendantsStatement = connection.prepareStatement(""
                + "SELECT o_subclass.iri "
                + "FROM extended_hierarchy "
                + "JOIN owl_objects AS o_subclass   ON o_subclass.id   = extended_hierarchy.subclass "
                + "JOIN owl_objects AS o_superclass ON o_superclass.id = extended_hierarchy.superclass "
                + "WHERE extension = ? AND o_superclass.id = ?");
        selectDescendantsStatement.setString(1, identifier);
        
        selectDescendantsSizeStatement = connection.prepareStatement(""
                + "SELECT COUNT(*) "
                + "FROM extended_hierarchy "
                + "JOIN owl_objects AS o_subclass   ON o_subclass.id   = extended_hierarchy.subclass "
                + "JOIN owl_objects AS o_superclass ON o_superclass.id = extended_hierarchy.superclass "
                + "WHERE extension = ? AND o_superclass.id = ?");
        selectDescendantsSizeStatement.setString(1, identifier);
        
        getMaxDepthStatement = connection.prepareStatement(""
                + "SELECT MAX(distance) "
                + "FROM extended_hierarchy "
                + "WHERE extension = ?");
        getMaxDepthStatement.setString(1, identifier);
        
        getDepthStatement = connection.prepareStatement(""
                + "SELECT MAX(distance) "
                + "FROM extended_hierarchy "
                + "WHERE extension = ? AND subclass = ?");
        getDepthStatement.setString(1, identifier);
        
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
                properties.add(factory.getOWLObjectProperty(IRI.create(string)));
            }
        }
        else if (key.equals("identifier")) {
            if (!element.isJsonPrimitive() && !element.getAsJsonPrimitive().isString())
                throw new JSONException("must be a string");
            identifier = element.getAsString();
            if (identifier.length() > 256)
                throw new JSONException("cannot have more than 256 characters");
        }
        else if (key.equals("emulate")) {
            if (!element.isJsonArray())
                throw new JSONException("must be a list");
            JsonArray array = element.getAsJsonArray();
            
            for (int i = 0; i < array.size(); i++) {
                JsonElement inner = array.get(i);
                if (!inner.isJsonPrimitive() || !inner.getAsJsonPrimitive().isString())
                    throw new JSONException("must be a string", "[" + i + "]");
                
                String string = inner.getAsString();
                if (string.equals("transitive")) {
                    if (emulateNotTransitive)
                        throw new JSONException("\"transitive\" and \"not transitive\" are mutually exclusive");
                    emulateTransitive = true;
                }
                else if (string.equals("not transitive")) {
                    if (emulateTransitive)
                        throw new JSONException("\"transitive\" and \"not transitive\" are mutually exclusive");
                    emulateNotTransitive = true;
                }
                else if (string.equals("reflexive")) {
                    if (emulateNotReflexive)
                        throw new JSONException("\"reflexive\" and \"not reflexive\" are mutually exclusive");
                    emulateReflexive = true;
                }
                else if (string.equals("not reflexive")) {
                    if (emulateReflexive)
                        throw new JSONException("\"reflexive\" and \"not reflexive\" are mutually exclusive");
                    emulateNotReflexive = true;
                }
                else
                    throw new JSONException("unknown emulation mode: \"" + string + "\"");
            }
        }
        else if (key.equals("subproperties")) {
            if (!element.isJsonPrimitive() && !element.getAsJsonPrimitive().isBoolean())
                throw new JSONException("must be a boolean");
            subProperties = element.getAsBoolean();
        }
        else
            super.processOption(key, element);
    }
    
    
    public int getDepth(OWLClass owlClass) throws SQLException {
        getDepthStatement.setInt(2, utils.getID(owlClass));
        try (ResultSet resultSet = getDepthStatement.executeQuery()) {
            if (resultSet.next())
                return resultSet.getInt(1);
        }
        return -1;
    }
    
    
    public int getMaxDepth() throws SQLException {
        try (ResultSet resultSet = getMaxDepthStatement.executeQuery()) {
            resultSet.next();
            return resultSet.getInt(1);
        }
    }
    
    
    public int getNumberOfSubclasses(OWLClass cls) throws SQLException {
        selectDescendantsSizeStatement.setInt(2, utils.getID(cls));
        try (ResultSet resultSet = selectDescendantsSizeStatement.executeQuery()) {
            resultSet.next();
            return resultSet.getInt(1);
        }
    }
    
    
    public int getNumberOfSuperclasses(OWLClass cls) throws SQLException {
        selectAncestrySizeStatement.setInt(2, utils.getID(cls));
        try (ResultSet resultSet = selectAncestrySizeStatement.executeQuery()) {
            resultSet.next();
            return resultSet.getInt(1);
        }
    }
    
    
    public HashSet<OWLClass> getSubclasses(OWLClass cls) throws SQLException {
        HashSet<OWLClass> result = new HashSet<>();
        selectDescendantsStatement.setInt(2, utils.getID(cls));
        try (ResultSet resultSet = selectDescendantsStatement.executeQuery()) {
            while (resultSet.next()) {
                String iri = resultSet.getString(1);
                result.add(factory.getOWLClass(IRI.create(iri)));
            }
        }
        
        return result;
    }
    
    
    public HashSet<OWLClass> getSuperclasses(OWLClass cls) throws SQLException {
        HashSet<OWLClass> result = new HashSet<>();
        selectAncestryStatement.setInt(2, utils.getID(cls));
        try (ResultSet resultSet = selectAncestryStatement.executeQuery()) {
            while (resultSet.next()) {
                String iri = resultSet.getString(1);
                result.add(factory.getOWLClass(IRI.create(iri)));
            }
        }
        
        return result;
    }
}
