package pt.owlsql.extractors;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import org.semanticweb.owlapi.model.EntityType;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntology;

import pt.json.JSONException;
import pt.owlsql.OWLExtractor;

import com.google.gson.JsonElement;


public final class IntrinsicICExtractor extends OWLExtractor {
    
    private final SQLCoreUtils utils = getExtractor(SQLCoreUtils.class);
    private final HierarchyExtractor ancestry = getExtractor(HierarchyExtractor.class);
    private final LeavesExtractor leaves = getExtractor(LeavesExtractor.class);
    
    private double zhouK;
    private double secoIC;
    private double zhouIC;
    private double sanchezIC;
    private double leavesIC;
    private int maxDepth;
    private double log_tC;
    private double log_tL;
    private double log_mD1;
    
    private PreparedStatement getICStatement;
    
    
    private void calculate(OWLClass owlClass) throws SQLException {
        if (owlClass == null) {
            secoIC = zhouIC = sanchezIC = leavesIC = 0;
            return;
        }
        
        int nDescendants = ancestry.getNumberOfSubclasses(owlClass);
        int nAncestors = ancestry.getNumberOfSuperclasses(owlClass);
        int nLeaves = leaves.getLeafDescendantsSize(owlClass);
        int depth = ancestry.getDepth(owlClass);
        
        double log_nD = Math.log(nDescendants);
        double log_nL = Math.log(nLeaves);
        
        // Formulas are correct but look different to increase calculation speed
        // They are also normalized so that all scores are form 0 to 1
        secoIC = 1 - log_nD / log_tC;
        zhouIC = zhouK * secoIC + (1 - zhouK) * Math.log(depth + 1) / log_mD1;
        sanchezIC = (log_tL + Math.log(nAncestors) - log_nL) / (log_tC + log_tL);
        leavesIC = 1 - log_nL / log_tL;
    }
    
    
    @SuppressWarnings("resource")
    @Override
    protected void extract(Set<OWLOntology> ontologies) throws SQLException {
        Connection connection = getConnection();
        
        Statement statement = connection.createStatement();
        statement.execute("DROP TABLE IF EXISTS intrinsic_ic");
        statement.execute("CREATE TABLE intrinsic_ic ("
                + "class INT,"
                + "seco DOUBLE,"
                + "zhou DOUBLE,"
                + "sanchez DOUBLE,"
                + "leaves DOUBLE,"
                + "UNIQUE (class))");
        statement.close();
        
        PreparedStatement insertStatement = connection.prepareStatement(""
                + "INSERT INTO intrinsic_ic (class, seco, zhou, sanchez, leaves) "
                + "VALUES (?, ?, ?, ?, ?)");
        
        System.out.println("Finding all the intrinsic IC values (SECO, ZHOU, SANCHEZ and LEAVES)");
        
        // Start by getting a reference to all the classes
        HashSet<OWLClass> allClasses = utils.getAllEntities(EntityType.CLASS);
        log_tC = Math.log(allClasses.size());
        log_tL = Math.log(leaves.getNumberOfLeaves());
        
        maxDepth = ancestry.getMaxDepth();
        log_mD1 = Math.log(maxDepth + 1);
        
        int counter = 0;
        for (OWLClass owlClass : allClasses) {
            calculate(owlClass);
            int id = utils.getID(owlClass);
            insertStatement.setInt(1, id);
            insertStatement.setDouble(2, secoIC);
            insertStatement.setDouble(3, zhouIC);
            insertStatement.setDouble(4, sanchezIC);
            insertStatement.setDouble(5, leavesIC);
            insertStatement.addBatch();
            
            counter++;
            if (counter % 1000 == 0) {
                System.out.println("... IC for " + counter + " classes found ...");
                insertStatement.executeBatch();
            }
        }
        
        insertStatement.executeBatch();
        insertStatement.close();
    }
    
    
    @Override
    protected String[] getMandatoryOptions() {
        return new String[] { "zhou_k" };
    }
    
    
    @Override
    protected void prepare() throws SQLException {
        Connection connection = getConnection();
        
        getICStatement = connection.prepareStatement(""
                + "SELECT seco, zhou, sanchez, leaves "
                + "FROM intrinsic_ic "
                + "WHERE class = ?");
    }
    
    
    @Override
    protected void processOption(String key, JsonElement element) throws JSONException {
        if (key.equals("zhou_k")) {
            if (!element.isJsonPrimitive() || !element.getAsJsonPrimitive().isNumber())
                throw new JSONException("must be a number");
            
            zhouK = element.getAsDouble();
            if (zhouK < 0 || zhouK > 1)
                throw new JSONException("must be a number between 0 and 1");
        }
        else {
            super.processOption(key, element);
        }
    }
    
    
    public double getIC(OWLClass cls, IntrinsicICMethod method) throws SQLException {
        getICStatement.setInt(1, utils.getID(cls));
        try (ResultSet resultsSet = getICStatement.executeQuery()) {
            if (resultsSet.next())
                if (method == IntrinsicICMethod.SECO)
                    return resultsSet.getDouble(1);
                else if (method == IntrinsicICMethod.ZHOU)
                    return resultsSet.getDouble(2);
                else if (method == IntrinsicICMethod.SANCHEZ)
                    return resultsSet.getDouble(3);
                else if (method == IntrinsicICMethod.LEAVES)
                    return resultsSet.getDouble(4);
        }
        
        return -1;
    }
}
