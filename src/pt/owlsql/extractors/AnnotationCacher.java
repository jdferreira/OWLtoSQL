package pt.owlsql.extractors;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;

import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLClass;

import pt.json.JSONException;
import pt.owlsql.Cacher;

import com.google.gson.JsonElement;


public final class AnnotationCacher extends Cacher {
    
    private BufferedReader fileReader;
    private boolean wipe = false;
    private String corpus;
    
    private final SQLCoreUtils utils = getExtractor(SQLCoreUtils.class);
    private PreparedStatement getTransitiveAnnotations;
    
    
    @Override
    protected void cache() throws SQLException {
        // We read the file line by line, split each line on whitespace into three parts and use each triplet to create
        // an annotation in the database.
        
        try (Statement statement = getConnection().createStatement()) {
            statement.execute(""
                    + "CREATE TABLE IF NOT EXISTS annotations ("
                    + "  id INT PRIMARY KEY AUTO_INCREMENT,"
                    + "  entity VARCHAR(256),"
                    + "  annotation INT,"
                    + "  corpus VARCHAR(256),"
                    + "  INDEX (entity),"
                    + "  INDEX (annotation),"
                    + "  INDEX (corpus))");
            
            if (wipe)
                statement.execute("TRUNCATE TABLE annotations");
        }
        
        @SuppressWarnings("resource")
        PreparedStatement insertAnnotation = getConnection()
                .prepareStatement("INSERT INTO annotations (entity, annotation, corpus) VALUES (?, ?, ?)");
        insertAnnotation.setString(3, corpus);
        
        String line;
        int lineNum = 0;
        int counter = 0;
        
        try {
            while ((line = fileReader.readLine()) != null) {
                line = line.trim();
                if (line.length() == 0 || line.startsWith("#"))
                    continue;
                
                lineNum++;
                String[] parts = line.split("\\s+", 3); // Ignore everything from the third column onwards!
                if (parts.length < 2) {
                    System.err.println("Ignoring line " + lineNum + ": expecting 2 columns; got " + parts.length);
                    continue;
                }
                
                String entity = parts[0];
                if (entity.length() > 256) {
                    System.err.println("Ignoring line "
                            + lineNum
                            + ": can only use entities whose name does not have more than 256 characters");
                    continue;
                }
                
                int annotationID = utils.getID(factory.getOWLClass(IRI.create(parts[1])));
                if (annotationID == -1) {
                    System.err.println("Ignoring line " + lineNum + ": unknown ontology term");
                    continue;
                }
                
                insertAnnotation.setString(1, entity);
                insertAnnotation.setInt(2, annotationID);
                insertAnnotation.addBatch();
                
                counter++;
                if (counter % 1000 == 0) {
                    System.out.println("... " + counter + " annotations found ...");
                    insertAnnotation.executeBatch();
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            insertAnnotation.executeBatch();
            insertAnnotation.close();
        }
    }
    
    
    @Override
    protected String[] getMandatoryOptions() {
        return new String[] { "file", "corpus" };
    }
    
    
    @Override
    protected void prepare() throws SQLException {
        Connection connection = getConnection();
        
        getTransitiveAnnotations = connection.prepareStatement(""
                + "SELECT DISTINCT hierarchy.superclass "
                + "FROM (SELECT DISTINCT annotation "
                + "      FROM annotations"
                + "      WHERE entity = ?"
                + "     ) AS t,"
                + "JOIN hierarchy ON hierarchy.subclass = t.annotation");
    }
    
    
    @Override
    protected void processOption(String key, JsonElement element) throws JSONException {
        if (key.equals("file")) {
            if (!element.isJsonPrimitive() || !element.getAsJsonPrimitive().isString())
                throw new JSONException("must be a string");
            try {
                fileReader = new BufferedReader(new FileReader(element.getAsString()));
            }
            catch (FileNotFoundException e) {
                throw new JSONException(e.getMessage(), e);
            }
        }
        else if (key.equals("wipe")) {
            if (!element.isJsonPrimitive() || !element.getAsJsonPrimitive().isBoolean())
                throw new JSONException("must be a boolean");
            wipe = element.getAsBoolean();
        }
        else if (key.equals("corpus")) {
            if (!element.isJsonPrimitive() || !element.getAsJsonPrimitive().isString())
                throw new JSONException("must be a string");
            corpus = element.getAsString();
            if (corpus.length() > 256)
                throw new JSONException("must have at most 256 characters");
        }
        else
            super.processOption(key, element);
    }
    
    
    public HashSet<OWLClass> getTransitiveAnnotations(String entity) throws SQLException {
        HashSet<OWLClass> result = new HashSet<>();
        
        for (int id : getTransitiveAnnotationsID(entity)) {
            result.add(utils.getEntity(id).asOWLClass());
        }
        
        return result;
    }
    
    
    public int[] getTransitiveAnnotationsID(String entity) throws SQLException {
        getTransitiveAnnotations.setString(1, entity);
        try (ResultSet resultSet = getTransitiveAnnotations.executeQuery()) {
            int[] result = new int[countRows(resultSet)];
            int i = 0;
            while (resultSet.next()) {
                result[i] = resultSet.getInt(1);
                i++;
            }
            return result;
        }
    }
}
