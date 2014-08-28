package pt.owlsql.extractors;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.semanticweb.owlapi.model.EntityType;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntology;

import pt.owlsql.OWLExtractor;


public class RelevanceExtractor extends OWLExtractor {
    
    private static final int MAX = Integer.MAX_VALUE;
    
    private final SQLCoreUtils utils = getExtractor(SQLCoreUtils.class);
    private final LeavesExtractor leaves = getExtractor(LeavesExtractor.class);
    private final IntrinsicICExtractor intrinsicIC = getExtractor(IntrinsicICExtractor.class);
    private final ExtrinsicICExtractor extrinsicIC = getExtractor(ExtrinsicICExtractor.class);
    
    private int totalLeaves;
    
    private int nChildren;
    private int nChildrenAdjusted;
    private int hIndex;
    private double ratioLeaves;
    private double ratioExternalSeco;
    private double ratioExternalZhou;
    private double ratioExternalSanchez;
    private double ratioExternalLeaves;
    
    private PreparedStatement getChildrenStatement;
    private PreparedStatement countChildrenStatement;
    
    
    private void calculate(int id) throws SQLException {
        int[] children = getChildren(id);
        
        nChildren = nChildrenAdjusted = children.length;
        if (leaves.isLeaf(id))
            nChildrenAdjusted = MAX;
        hIndex = getHIndex(children);
        
        int nLeaves = leaves.getLeafDescendantsSize(id);
        ratioLeaves = nLeaves / totalLeaves;
        
        double extrinsicICValue = extrinsicIC.getIC(id);
        ratioExternalSeco = extrinsicICValue / intrinsicIC.getIC(id, IntrinsicICMethod.SECO);
        ratioExternalZhou = extrinsicICValue / intrinsicIC.getIC(id, IntrinsicICMethod.ZHOU);
        ratioExternalSanchez = extrinsicICValue / intrinsicIC.getIC(id, IntrinsicICMethod.SANCHEZ);
        ratioExternalLeaves = extrinsicICValue / intrinsicIC.getIC(id, IntrinsicICMethod.LEAVES);
    }
    
    
    private int countChildren(int id) throws SQLException {
        countChildrenStatement.setInt(1, id);
        try (ResultSet resultSet = getChildrenStatement.executeQuery()) {
            if (resultSet.next())
                return resultSet.getInt(1);
        }
        
        return 0;
    }
    
    
    private int[] getChildren(int id) throws SQLException {
        getChildrenStatement.setInt(1, id);
        
        try (ResultSet resultSet = getChildrenStatement.executeQuery()) {
            int[] result = new int[countRows(resultSet)];
            int i = 0;
            while (resultSet.next()) {
                result[i] = resultSet.getInt(1);
                i++;
            }
            return result;
        }
    }
    
    
    private int getHIndex(int[] children) throws SQLException {
        if (children.length == 0)
            return MAX;
        
        // For each children, count the number of their children
        int[] grandchildren = new int[children.length];
        for (int i = 0; i < children.length; i++) {
            grandchildren[i] = countChildren(children[i]);
        }
        
        Arrays.sort(grandchildren);
        
        if (grandchildren[grandchildren.length - 1] == 0)
            // All children have 0 children of their own; so this class has the maximum relevance
            return MAX;
        
        int result = 0;
        for (int i = 1; i <= grandchildren.length; i++) {
            int index = grandchildren.length - i;
            if (grandchildren[index] >= i)
                result++;
            else
                break;
        }
        return result;
    }
    
    
    @Override
    protected void extract(Set<OWLOntology> ontologies) throws SQLException {
        // In here, we will calculate the several relevance factors
        Connection connection = getConnection();
        
        try (Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS relevance");
            statement.execute("" //
                    + "CREATE TABLE relevance ("
                    + "    class INT PRIMARY KEY, "
                    + "    n_children INT, " // Number of direct children
                    + "    n_children_adjusted INT, " // Number of direct children, or MAX for leaves
                    + "    h_index INT, " // h <=> class has at least h children with h children each; or MAX for leaves
                                          // and leaf parents
                    + "    ratio_leaves DOUBLE, " // Number of leaves / Total number of leaves
                    + "    ratio_external_seco DOUBLE, " // These are IC ratios between external and internal measures
                    + "    ratio_external_zhou DOUBLE, "
                    + "    ratio_external_sanchez DOUBLE, "
                    + "    ratio_external_leaves DOUBLE"
                    + ")");
        }
        
        getChildrenStatement = connection.prepareStatement(""
                + "SELECT subclass "
                + "FROM hierarchy "
                + "WHERE superclass = ?"
                + "  AND distance = 1");
        countChildrenStatement = connection.prepareStatement(""
                + "SELECT COUNT(subclass) "
                + "FROM hierarchy "
                + "WHERE superclass = ?"
                + "  AND distance = 1");
        
        try (PreparedStatement insertStatement = connection.prepareStatement(""
                + "INSERT INTO relevance ("
                + "    class, "
                + "    n_children, "
                + "    n_children_adjusted, "
                + "    h_index, "
                + "    ratio_leaves, "
                + "    ratio_external_seco, "
                + "    ratio_external_zhou, "
                + "    ratio_external_sanchez, "
                + "    ratio_external_leaves)"
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            
            System.out.println("Finding all the intrinsic IC values (SECO, ZHOU, SANCHEZ and LEAVES)");
            
            // Start by getting a reference to all the classes
            HashSet<OWLClass> allClasses = utils.getAllEntities(EntityType.CLASS);
            totalLeaves = leaves.getNumberOfLeaves();
            
            int counter = 0;
            for (OWLClass owlClass : allClasses) {
                int id = utils.getID(owlClass);
                if (id == -1)
                    continue;
                
                calculate(id);
                insertStatement.setInt(1, id);
                insertStatement.setDouble(2, nChildren);
                insertStatement.setDouble(3, nChildrenAdjusted);
                insertStatement.setDouble(4, hIndex);
                insertStatement.setDouble(5, ratioLeaves);
                insertStatement.setDouble(6, ratioExternalSeco);
                insertStatement.setDouble(7, ratioExternalZhou);
                insertStatement.setDouble(8, ratioExternalSanchez);
                insertStatement.setDouble(9, ratioExternalLeaves);
                insertStatement.addBatch();
                
                counter++;
                if (counter % 1000 == 0) {
                    System.out.println("... IC for " + counter + " classes found ...");
                    insertStatement.executeBatch();
                }
            }
            
            insertStatement.executeBatch();
        }
        
    }
    
    
    @Override
    protected void prepare() throws SQLException {}
}
