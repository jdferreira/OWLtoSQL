package pt.owlsql.extractors.relations;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import org.semanticweb.owlapi.model.EntityType;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLOntology;

import pt.owlsql.OWLExtractor;
import pt.owlsql.extractors.SQLCoreUtils;


public final class RelationsExtractor extends OWLExtractor {
    
    private final SQLCoreUtils utils;
    private PreparedStatement getRelationsStatement;
    
    
    public RelationsExtractor() throws SQLException {
        utils = getExtractor(SQLCoreUtils.class);
    }
    
    
    @SuppressWarnings("resource")
    @Override
    protected void extract(Set<OWLOntology> ontologies) throws SQLException {
        Connection connection = getConnection();
        
        Statement statement = connection.createStatement();
        statement.execute("DROP TABLE IF EXISTS relations");
        statement.execute("CREATE TABLE relations ("
                + "start INT,"
                + "chain TEXT,"
                + "end INT,"
                + "INDEX (chain(256)))");
        statement.close();
        
        PreparedStatement insertStatement = connection.prepareStatement(""
                + "INSERT INTO relations (start, chain, end) "
                + "VALUES (?, ?, ?)");
        
        System.out.println("Finding the direct relations between pairs of concepts");
        
        // Start by getting a reference to all the classes
        HashSet<OWLClass> allClasses = utils.getAllEntities(EntityType.CLASS);
        
        // And an Unfolder object
        Unfolder unfolder = new Unfolder();
        
        int counter = 0;
        for (OWLClass owlClass : allClasses) {
            Set<OWLClassExpression> superclasses = owlClass.getSuperClasses(ontologies);
            superclasses.addAll(owlClass.getEquivalentClasses(ontologies));
            
            RelationsStore allRelations = new RelationsStore();
            for (OWLClassExpression superclass : superclasses) {
                if (superclass.isAnonymous())
                    allRelations.addAll(superclass.accept(unfolder));
            }
            
            int id = utils.getID(owlClass);
            insertStatement.setInt(1, id);
            for (Chain chain : allRelations) {
                if (chain.propertiesLength() == 0)
                    continue;
                
                OWLObjectProperty[] properties = chain.getChain();
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < properties.length; i++) {
                    if (i > 0)
                        sb.append(",");
                    sb.append(utils.getID(properties[i]));
                }
                insertStatement.setString(2, sb.toString());
                
                int endID = utils.getID(chain.getEndPoint());
                insertStatement.setInt(3, endID);
                insertStatement.addBatch();
            }
            
            counter++;
            if (counter % 1000 == 0) {
                System.out.println("... relations for " + counter + " classes found ...");
                insertStatement.executeBatch();
            }
        }
        
        insertStatement.executeBatch();
        insertStatement.close();
    }
    
    
    public RelationsStore getRelations(OWLClass cls) throws SQLException {
        RelationsStore result = new RelationsStore();
        
        getRelationsStatement.setInt(1, utils.getID(cls));
        try (ResultSet resultSet = getRelationsStatement.executeQuery()) {
            while (resultSet.next()) {
                String chainString = resultSet.getString(1);
                int endID = resultSet.getInt(2);
                
                String[] fields = chainString.split(",");
                OWLObjectProperty[] properties = new OWLObjectProperty[fields.length];
                for (int i = 0; i < properties.length; i++) {
                    properties[i] = utils.getEntity(Integer.parseInt(fields[i])).asOWLObjectProperty();
                }
                OWLClass endPoint = utils.getEntity(endID).asOWLClass();
                
                result.add(new Chain(properties, endPoint));
            }
        }
        
        return result;
    }
    
    
    @Override
    public void prepare() throws SQLException {
        Connection connection = getConnection();
        getRelationsStatement = connection.prepareStatement("SELECT chain, end FROM relations WHERE start = ?");
    }
}
