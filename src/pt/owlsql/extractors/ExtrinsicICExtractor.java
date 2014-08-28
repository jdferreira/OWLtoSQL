package pt.owlsql.extractors;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntology;

import pt.owlsql.OWLExtractor;


public final class ExtrinsicICExtractor extends OWLExtractor {
    
    private final SQLCoreUtils utils = getExtractor(SQLCoreUtils.class);
    
    private PreparedStatement getICStatement;
    
    
    @SuppressWarnings("resource")
    @Override
    protected void extract(Set<OWLOntology> ontologies) throws SQLException {
        Connection connection = getConnection();
        
        int nModels;
        try (Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS extrinsic_ic");
            statement.execute("CREATE TABLE extrinsic_ic (class INT PRIMARY KEY, ic DOUBLE)");
            
            // Get the number of all annotated entities
            try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(DISTINCT entity) FROM annotations")) {
                resultSet.next();
                nModels = resultSet.getInt(1);
            }
        }
        
        System.out.println("Computing the extrinsic IC values for all concepts");
        
        PreparedStatement insertStatement = connection.prepareStatement(""
                + "INSERT INTO extrinsic_ic (class, ic) "
                + "SELECT superclass, 1 - LOG(COUNT(DISTINCT entity)) / LOG(?)"
                + "FROM annotations "
                + "JOIN hierarchy ON hierarchy.subclass = annotations.annotation "
                + "GROUP BY superclass");
        insertStatement.setInt(1, nModels);
        insertStatement.executeUpdate();
        insertStatement.close();
    }
    
    
    @Override
    protected void prepare() throws SQLException {
        getICStatement = getConnection().prepareStatement("SELECT ic FROM extrinsic_ic WHERE class = ?");
    }
    
    
    public double getIC(OWLClass cls) throws SQLException {
        return getIC(utils.getID(cls));
    }
    
    
    public double getIC(int id) throws SQLException {
        getICStatement.setInt(1, id);
        try (ResultSet resultsSet = getICStatement.executeQuery()) {
            return resultsSet.getDouble(1);
        }
    }
}
