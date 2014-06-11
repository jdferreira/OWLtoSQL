package pt.owlsql.extractors;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import org.semanticweb.owlapi.model.AxiomType;
import org.semanticweb.owlapi.model.EntityType;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom;

import pt.owlsql.OWLExtractor;


public final class HierarchyExtractor extends OWLExtractor {
    
    private static final OWLClass OWL_THING = factory.getOWLThing();
    
    private PreparedStatement getDepthStatement;
    
    private PreparedStatement getMaxDepthStatement;
    private PreparedStatement selectAncestrySizeStatement;
    private PreparedStatement selectAncestryStatement;
    private PreparedStatement selectDescendantsSizeStatement;
    private PreparedStatement selectDescendantsStatement;
    private final SQLCoreUtils utils = getExtractor(SQLCoreUtils.class);
    
    
    @SuppressWarnings("resource")
    @Override
    protected void extract(Set<OWLOntology> ontologies) throws SQLException {
        // TODO Do we want to respect the equivalences between named classes of the original OWL and put them into the
        // hierarchy as well??
        
        Connection connection = getConnection();
        
        Statement statement = connection.createStatement();
        statement.execute("DROP TABLE IF EXISTS hierarchy");
        statement.execute("CREATE TABLE hierarchy ("
                + "subclass INT,"
                + "superclass INT,"
                + "distance INT,"
                + "INDEX (subclass),"
                + "INDEX (superclass),"
                + "INDEX (distance),"
                + "UNIQUE (subclass, superclass))");
        statement.close();
        
        PreparedStatement insertStatement = connection.prepareStatement(""
                + "INSERT IGNORE INTO hierarchy (subclass, superclass, distance) "
                + "VALUES (?, ?, ?)");
        
        System.out.println("Finding all the classes");
        
        // Start by getting a reference to all the classes
        HashSet<OWLClass> noSuperclass = utils.getAllEntities(EntityType.CLASS);
        
        // And by saying that everything is a subclass of itself (with distance 0)
        insertStatement.setInt(3, 0); // The distance will be 0 here
        for (OWLClass owlClass : noSuperclass) {
            int id = utils.getID(owlClass);
            insertStatement.setInt(1, id);
            insertStatement.setInt(2, id);
            insertStatement.addBatch();
        }
        insertStatement.executeBatch();
        
        // We now go through each subclassOf axiom and populate the first part of the hierarchy
        System.out.println("Finding direct class-subclass relations");
        int counter = 0;
        
        insertStatement.setInt(3, 1); // The distance will be 1 for all direct axioms
        for (OWLOntology ontology : ontologies) {
            Set<OWLSubClassOfAxiom> axioms = ontology.getAxioms(AxiomType.SUBCLASS_OF, true);
            for (OWLSubClassOfAxiom axiom : axioms) {
                OWLClassExpression subClass = axiom.getSubClass();
                OWLClassExpression superClass = axiom.getSuperClass();
                
                if (subClass.isAnonymous() || superClass.isAnonymous())
                    continue;
                
                OWLClass subOWLClass = subClass.asOWLClass();
                OWLClass superOWLClass = superClass.asOWLClass();
                counter++;
                
                // We found a class with a superclass, so remove it from the set of noSuperclass classes
                noSuperclass.remove(subOWLClass);
                
                // Add this information to the database
                int subClassID = utils.getID(subOWLClass);
                int superClassID = utils.getID(superOWLClass);
                insertStatement.setInt(1, subClassID);
                insertStatement.setInt(2, superClassID);
                insertStatement.addBatch();
                
                if (counter % 1000 == 0)
                    System.out.println("... found " + counter + " direct relations by now ...");
            }
        }
        
        // Those classes that do not have a superclass should be now processed so that owl:Thing is their superclass
        // This includes owl:Thing, which is part of the noSuperclass set
        insertStatement.setInt(2, utils.getID(OWL_THING)); // Set the superclass to 1
        insertStatement.setInt(3, 1); // Set the distance to 1
        for (OWLClass owlClass : noSuperclass) {
            // Store this information on memory
            counter++;
            
            // Add this information to the database
            int subClassID = utils.getID(owlClass);
            insertStatement.setInt(1, subClassID);
            insertStatement.addBatch();
        }
        System.out.println(counter + " direct relations");
        
        insertStatement.executeBatch();
        insertStatement.close();
        
        PreparedStatement newInsertDistance = connection.prepareStatement(""
                + "INSERT IGNORE INTO hierarchy (subclass, superclass, distance)"
                + "  SELECT h1.subclass, h2.superclass, h1.distance + 1"
                + "  FROM hierarchy AS h1, hierarchy AS h2"
                + "  WHERE h1.superclass = h2.subclass AND"
                + "        h1.distance = ? AND"
                + "        h2.distance = 1");
        
        int distance = 1;
        while (true) {
            newInsertDistance.setInt(1, distance);
            int newRows = newInsertDistance.executeUpdate();
            System.out.println(newRows + " relations with distance = " + (distance + 1));
            if (newRows == 0)
                break;
            distance++;
        }
        
        newInsertDistance.close();
    }
    
    
    @Override
    protected void prepare() throws SQLException {
        Connection connection = getConnection();
        
        selectAncestryStatement = connection.prepareStatement(""
                + "SELECT o_superclass.iri "
                + "FROM hierarchy "
                + "JOIN owl_objects AS o_subclass   ON o_subclass.id   = hierarchy.subclass "
                + "JOIN owl_objects AS o_superclass ON o_superclass.id = hierarchy.superclass "
                + "WHERE o_subclass.id = ?");
        selectAncestrySizeStatement = connection.prepareStatement(""
                + "SELECT COUNT(*) "
                + "FROM hierarchy "
                + "JOIN owl_objects AS o_subclass   ON o_subclass.id   = hierarchy.subclass "
                + "JOIN owl_objects AS o_superclass ON o_superclass.id = hierarchy.superclass "
                + "WHERE o_subclass.id = ?");
        selectDescendantsStatement = connection.prepareStatement(""
                + "SELECT o_subclass.iri "
                + "FROM hierarchy "
                + "JOIN owl_objects AS o_subclass   ON o_subclass.id   = hierarchy.subclass "
                + "JOIN owl_objects AS o_superclass ON o_superclass.id = hierarchy.superclass "
                + "WHERE o_superclass.id = ?");
        selectDescendantsSizeStatement = connection.prepareStatement(""
                + "SELECT COUNT(*) "
                + "FROM hierarchy "
                + "JOIN owl_objects AS o_subclass   ON o_subclass.id   = hierarchy.subclass "
                + "JOIN owl_objects AS o_superclass ON o_superclass.id = hierarchy.superclass "
                + "WHERE o_superclass.id = ?");
        getMaxDepthStatement = connection.prepareStatement("SELECT MAX(distance) FROM hierarchy");
        getDepthStatement = connection.prepareStatement("SELECT MAX(distance) FROM hierarchy WHERE subclass = ?");
    }
    
    
    public int getDepth(OWLClass owlClass) throws SQLException {
        getDepthStatement.setInt(1, utils.getID(owlClass));
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
        int id = utils.getID(cls);
        
        selectDescendantsSizeStatement.setInt(1, id);
        try (ResultSet resultSet = selectDescendantsSizeStatement.executeQuery()) {
            resultSet.next();
            return resultSet.getInt(1);
        }
    }
    
    
    public int getNumberOfSuperclasses(OWLClass cls) throws SQLException {
        int id = utils.getID(cls);
        
        selectAncestrySizeStatement.setInt(1, id);
        try (ResultSet resultSet = selectAncestrySizeStatement.executeQuery()) {
            resultSet.next();
            return resultSet.getInt(1);
        }
    }
    
    
    public HashSet<OWLClass> getSubclasses(OWLClass cls) throws SQLException {
        int id = utils.getID(cls);
        
        HashSet<OWLClass> result = new HashSet<>();
        selectDescendantsStatement.setInt(1, id);
        try (ResultSet resultSet = selectDescendantsStatement.executeQuery()) {
            while (resultSet.next()) {
                String iri = resultSet.getString(1);
                result.add(factory.getOWLClass(IRI.create(iri)));
            }
        }
        
        return result;
    }
    
    
    public HashSet<OWLClass> getSuperclasses(OWLClass cls) throws SQLException {
        int id = utils.getID(cls);
        
        HashSet<OWLClass> result = new HashSet<>();
        selectAncestryStatement.setInt(1, id);
        try (ResultSet resultSet = selectAncestryStatement.executeQuery()) {
            while (resultSet.next()) {
                String iri = resultSet.getString(1);
                result.add(factory.getOWLClass(IRI.create(iri)));
            }
        }
        
        return result;
    }
}
