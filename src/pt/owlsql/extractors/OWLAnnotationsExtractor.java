package pt.owlsql.extractors;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import org.semanticweb.owlapi.model.AxiomType;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLObjectSomeValuesFrom;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

import pt.json.JSONException;
import pt.owlsql.OWLExtractor;


public final class OWLAnnotationsExtractor extends OWLExtractor {
    
    private final SQLCoreUtils utils;
    private PreparedStatement getTransitiveEntities;
    private PreparedStatement getTransitiveAnnotations;
    
    private final HashSet<OWLObjectProperty> properties = new HashSet<>();
    private boolean update;
    
    
    public OWLAnnotationsExtractor() throws SQLException {
        utils = getExtractor(SQLCoreUtils.class);
    }
    
    
    @Override
    protected void extract(Set<OWLOntology> ontologies) throws SQLException {
        @SuppressWarnings("resource")
        Statement statement = getConnection().createStatement();
        
        statement.execute("CREATE TABLE IF NOT EXISTS owl_annotations ("
                + "entity INT,"
                + "property INT,"
                + "annotation INT,"
                + "INDEX (entity),"
                + "INDEX (property),"
                + "INDEX (annotation))");
        
        if (!update)
            statement.execute("TRUNCATE TABLE owl_annotations");
        
        statement.close();
        
        @SuppressWarnings("resource")
        PreparedStatement insertAnnotation = getConnection()
                .prepareStatement("INSERT INTO owl_annotations (entity, property, annotation) VALUES (?, ?, ?)");
        
        int counter = 0;
        for (OWLOntology ontology : ontologies) {
            Set<OWLSubClassOfAxiom> axioms = ontology.getAxioms(AxiomType.SUBCLASS_OF, true);
            for (OWLSubClassOfAxiom axiom : axioms) {
                OWLClassExpression subclass = axiom.getSubClass();
                OWLClassExpression superclass = axiom.getSuperClass();
                
                if (subclass.isAnonymous() || !(superclass instanceof OWLObjectSomeValuesFrom))
                    continue;
                
                OWLObjectSomeValuesFrom expr = (OWLObjectSomeValuesFrom) superclass;
                if (expr.getFiller().isAnonymous() || !properties.contains(expr.getProperty()))
                    continue;
                
                OWLClass entity = subclass.asOWLClass();
                OWLObjectProperty property = expr.getProperty().asOWLObjectProperty();
                OWLClass annotation = expr.getFiller().asOWLClass();
                int entityID = utils.getID(entity);
                int propertyID = utils.getID(property);
                int annotationID = utils.getID(annotation);
                
                insertAnnotation.setInt(1, entityID);
                insertAnnotation.setInt(2, propertyID);
                insertAnnotation.setInt(3, annotationID);
                insertAnnotation.addBatch();
                
                counter++;
                if (counter % 1000 == 0) {
                    System.out.println("... annotations for " + counter + " classes found ...");
                    insertAnnotation.executeBatch();
                }
            }
        }
        
        insertAnnotation.executeBatch();
        insertAnnotation.close();
    }
    
    
    @Override
    protected String[] getMandatoryOptions() {
        return new String[] { "properties", "update" };
    }
    
    
    @Override
    protected void prepare() throws SQLException {
        Connection connection = getConnection();
        
        getTransitiveEntities = connection.prepareStatement(""
                + "SELECT owl_annotations.entity "
                + "FROM hierarchy "
                + "JOIN owl_annotations ON owl_annotations.annotation = hierarchy.subclass "
                + "WHERE hierarchy.superclass = ? AND owl_annotations.property = ?");
        
        getTransitiveAnnotations = connection.prepareStatement(""
                + "SELECT hierarchy.superclass "
                + "FROM owl_annotations "
                + "JOIN hierarchy ON hierarchy.subclass = owl_annotations.entity "
                + "WHERE owl_annotations.entity = ? AND owl_annotations.property = ?");
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
        else if (key.equals("update")) {
            if (!element.isJsonPrimitive() || !element.getAsJsonPrimitive().isBoolean())
                throw new JSONException("must be a boolean");
            update = element.getAsBoolean();
        }
        else
            super.processOption(key, element);
    }
    
    
    public HashSet<OWLClass> getTransitiveAnnotations(OWLClass owlClass, OWLObjectProperty property)
            throws SQLException {
        HashSet<OWLClass> result = new HashSet<>();
        
        int classID = utils.getID(owlClass);
        int propertyID = utils.getID(property);
        
        getTransitiveAnnotations.setInt(1, classID);
        getTransitiveAnnotations.setInt(2, propertyID);
        
        try (ResultSet resultSet = getTransitiveAnnotations.executeQuery()) {
            while (resultSet.next()) {
                result.add(factory.getOWLClass(IRI.create(resultSet.getString(1))));
            }
        }
        
        return result;
    }
    
    
    public HashSet<OWLClass> getTransitiveClassesWithAnnotation(OWLObjectProperty property, OWLClass annotation)
            throws SQLException {
        HashSet<OWLClass> result = new HashSet<>();
        
        int propertyID = utils.getID(property);
        int annotationID = utils.getID(annotation);
        
        getTransitiveEntities.setInt(1, annotationID);
        getTransitiveEntities.setInt(2, propertyID);
        
        try (ResultSet resultSet = getTransitiveEntities.executeQuery()) {
            while (resultSet.next()) {
                result.add(factory.getOWLClass(IRI.create(resultSet.getString(1))));
            }
        }
        
        return result;
    }
}
