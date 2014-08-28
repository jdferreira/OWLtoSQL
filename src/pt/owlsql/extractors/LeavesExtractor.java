package pt.owlsql.extractors;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntology;

import pt.owlsql.OWLExtractor;


public final class LeavesExtractor extends OWLExtractor {
    
    private final SQLCoreUtils utils = getExtractor(SQLCoreUtils.class);
    
    private PreparedStatement isLeafStatement;
    private PreparedStatement getLeaves;
    private PreparedStatement getLeavesSize;
    private PreparedStatement getNumberOfLeaves;
    
    
    @Override
    protected void extract(Set<OWLOntology> ontologies) throws SQLException {
        @SuppressWarnings("resource")
        final Statement statement = getConnection().createStatement();
        
        // Create the table that contains the IRI's of OWLEntities
        statement.execute("DROP TABLE IF EXISTS leaves");
        statement.execute("CREATE TABLE leaves (id INT, UNIQUE (id))");
        
        statement.execute(""
                + "INSERT INTO leaves (id) "
                + "SELECT superclass "
                + "FROM hierarchy "
                + "GROUP BY superclass "
                + "HAVING COUNT(*) = 1");
        
        statement.close();
    }
    
    
    @Override
    protected void prepare() throws SQLException {
        final Connection connection = getConnection();
        
        isLeafStatement = connection.prepareStatement("SELECT COUNT(*) FROM leaves WHERE id = ?");
        getLeaves = connection.prepareStatement(""
                + "SELECT subclass "
                + "FROM hierarchy "
                + "JOIN leaves ON leaves.id = subclass "
                + "WHERE superclass = ?");
        getLeavesSize = connection.prepareStatement(""
                + "SELECT COUNT(*) "
                + "FROM hierarchy "
                + "JOIN leaves ON leaves.id = subclass "
                + "WHERE superclass = ?");
        getNumberOfLeaves = connection.prepareStatement("SELECT COUNT(*) FROM leaves");
    }
    
    
    public HashSet<OWLClass> getLeafDescendants(int id) throws SQLException {
        getLeaves.setInt(1, id);
        final HashSet<OWLClass> result = new HashSet<>();
        try (ResultSet resultSet = getLeaves.executeQuery()) {
            while (resultSet.next())
                result.add((OWLClass) utils.getEntity(resultSet.getInt(1)));
        }
        return result;
    }
    
    
    public HashSet<OWLClass> getLeafDescendants(OWLClass owlClass) throws SQLException {
        return getLeafDescendants(utils.getID(owlClass));
    }
    
    
    public int getLeafDescendantsSize(int id) throws SQLException {
        getLeavesSize.setInt(1, id);
        try (ResultSet resultSet = getLeavesSize.executeQuery()) {
            resultSet.next();
            return resultSet.getInt(1);
        }
    }
    
    
    public int getLeafDescendantsSize(OWLClass owlClass) throws SQLException {
        return getLeafDescendantsSize(utils.getID(owlClass));
    }
    
    
    public int getNumberOfLeaves() throws SQLException {
        try (ResultSet resultSet = getNumberOfLeaves.executeQuery()) {
            resultSet.next();
            return resultSet.getInt(1);
        }
    }
    
    
    public boolean isLeaf(int id) throws SQLException {
        isLeafStatement.setInt(1, id);
        try (ResultSet resultSet = isLeafStatement.executeQuery()) {
            resultSet.next();
            return resultSet.getInt(1) == 0;
        }
    }
    
    
    // TODO All methods that take OWLClass should also take int
    public boolean isLeaf(OWLClass owlClass) throws SQLException {
        return isLeaf(utils.getID(owlClass));
    }
}
