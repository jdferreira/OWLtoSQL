package pt.owlsql.extractors;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Set;

import org.semanticweb.owlapi.model.AxiomType;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLDisjointClassesAxiom;
import org.semanticweb.owlapi.model.OWLOntology;

import pt.owlsql.OWLExtractor;


public final class DisjointnessExtractor extends OWLExtractor {
    
    public static class IntPair {
        private int first;
        private int second;
        
        
        public IntPair(int first, int second) {
            this.first = first;
            this.second = second;
        }
        
        
        public int getFirst() {
            return first;
        }
        
        
        public int getSecond() {
            return second;
        }
    }
    
    
    public static class OWLClassPair {
        private OWLClass first;
        private OWLClass second;
        
        
        public OWLClassPair(OWLClass first, OWLClass second) {
            this.first = first;
            this.second = second;
        }
        
        
        public OWLClass getFirst() {
            return first;
        }
        
        
        public OWLClass getSecond() {
            return second;
        }
    }
    
    
    private final SQLCoreUtils utils = getExtractor(SQLCoreUtils.class);
    private PreparedStatement getDisjointStatement;
    private PreparedStatement areDisjointStatement;
    
    
    @Override
    protected void extract(Set<OWLOntology> ontologies) throws SQLException {
        @SuppressWarnings("resource")
        Statement statement = getConnection().createStatement();
        
        statement.execute("DROP TABLE IF EXISTS disjoints");
        statement.execute(""
                + "CREATE TABLE disjoints ("
                + "    id1 INT,"
                + "    id2 INT, "
                + "    INDEX (id1),"
                + "    INDEX (id2),"
                + "    UNIQUE (id1, id2))");
        
        statement.close();
        
        @SuppressWarnings("resource")
        PreparedStatement insertAnnotation = getConnection()
                .prepareStatement("INSERT IGNORE INTO disjoints (id1, id2) VALUES (?, ?)");
        
        int counter = 0;
        for (OWLOntology ontology : ontologies) {
            Set<OWLDisjointClassesAxiom> axioms = ontology.getAxioms(AxiomType.DISJOINT_CLASSES, true);
            for (OWLDisjointClassesAxiom axiom : axioms) {
                ArrayList<OWLClassExpression> expressions = new ArrayList<>(axiom.getClassExpressionsAsList());
                for (int i = 0; i < expressions.size(); i++) {
                    OWLClassExpression ce1 = expressions.get(i);
                    if (ce1.isAnonymous())
                        continue;
                    int id1 = utils.getID(ce1.asOWLClass());
                    
                    for (int j = i + 1; j < expressions.size(); j++) {
                        OWLClassExpression ce2 = expressions.get(j);
                        if (ce2.isAnonymous())
                            continue;
                        int id2 = utils.getID(ce2.asOWLClass());
                        
                        if (id1 == id2) {
                            System.out.println("WARNING: " + ce1 + " is asserted disjoint with itself on " + ontology);
                        }
                        else if (id1 > id2) {
                            // Exchange them
                            int tmp = id1;
                            id1 = id2;
                            id2 = tmp;
                        }
                        
                        insertAnnotation.setInt(1, id1);
                        insertAnnotation.setInt(2, id2);
                        insertAnnotation.addBatch();
                        
                        counter++;
                        if (counter % 1000 == 0) {
                            System.out.println("... " + counter + " disjoint classes found ...");
                            insertAnnotation.executeBatch();
                        }
                    }
                }
            }
        }
        
        insertAnnotation.executeBatch();
        insertAnnotation.close();
    }
    
    
    @Override
    protected void prepare() throws SQLException {
        Connection connection = getConnection();
        
        // Notice that we always insert into the database with id1 < id2.
        
        // Apparently, to make use of the indexes of the table, we must not use a WHERE clause with an
        // OR expression. Instead, we use a UNION of two SELECT statements
        getDisjointStatement = connection.prepareStatement(""
                + "SELECT g1.superclass, g2.superclass "
                + "FROM graphpath AS g1, graphpath AS g2, disjoints "
                + "WHERE g1.subclass = ? AND g2.subclass = ? "
                + "      AND disjoints.id1 = g1.superclass AND disjoints.id2 = g2.superclass "
                + "UNION "
                + "SELECT g1.superclass, g2.superclass "
                + "FROM graphpath AS g1, graphpath AS g2, disjoints "
                + "WHERE g1.subclass = ? AND g2.subclass = ? "
                + "      AND disjoints.id1 = g2.superclass AND disjoints.id2 = g1.superclass");
        
        areDisjointStatement = connection.prepareStatement(""
                + "SELECT EXISTS ("
                + "    SELECT g1.superclass, g2.superclass "
                + "    FROM graphpath AS g1, graphpath AS g2, disjoints "
                + "    WHERE g1.subclass = ? AND g2.subclass = ? "
                + "          AND disjoints.id1 = g1.superclass AND disjoints.id2 = g2.superclass "
                + "    UNION "
                + "    SELECT g1.superclass, g2.superclass "
                + "    FROM graphpath AS g1, graphpath AS g2, disjoints "
                + "    WHERE g1.subclass = ? AND g2.subclass = ? "
                + "          AND disjoints.id1 = g2.superclass AND disjoints.id2 = g1.superclass)");
    }
    
    
    public boolean areDisjoint(int id1, int id2) throws SQLException {
        areDisjointStatement.setInt(1, id1);
        areDisjointStatement.setInt(2, id2);
        areDisjointStatement.setInt(3, id1);
        areDisjointStatement.setInt(4, id2);
        
        try (ResultSet resultSet = areDisjointStatement.executeQuery()) {
            return resultSet.next() && resultSet.getBoolean(1);
        }
    }
    
    
    public boolean areDisjoint(OWLClass cls1, OWLClass cls2) throws SQLException {
        return areDisjoint(utils.getID(cls1), utils.getID(cls2));
    }
    
    
    public ArrayList<IntPair> getDisjointSuperclasses(int id1, int id2) throws SQLException {
        getDisjointStatement.setInt(1, id1);
        getDisjointStatement.setInt(2, id2);
        getDisjointStatement.setInt(3, id1);
        getDisjointStatement.setInt(4, id2);
        
        ArrayList<IntPair> result = new ArrayList<>();
        try (ResultSet resultSet = getDisjointStatement.executeQuery()) {
            while (resultSet.next()) {
                result.add(new IntPair(resultSet.getInt(1), resultSet.getInt(2)));
            }
        }
        
        return result;
    }
    
    
    public ArrayList<OWLClassPair> getDisjointSuperclasses(OWLClass cls1, OWLClass cls2) throws SQLException {
        int id1 = utils.getID(cls1);
        int id2 = utils.getID(cls2);

        ArrayList<IntPair> resultWithInts = getDisjointSuperclasses(id1, id2);
        ArrayList<OWLClassPair> result = new ArrayList<>();
        
        for (IntPair intPair : resultWithInts) {
            OWLClass superclass1 = utils.getEntity(intPair.getFirst()).asOWLClass();
            OWLClass superclass2 = utils.getEntity(intPair.getSecond()).asOWLClass();
            result.add(new OWLClassPair(superclass1, superclass2));
        }
        
        return result;
    }
}
