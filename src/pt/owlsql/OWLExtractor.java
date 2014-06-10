package pt.owlsql;

import java.sql.SQLException;
import java.util.Set;

import org.semanticweb.owlapi.model.OWLOntology;


public abstract class OWLExtractor extends Extractor {
    
    protected abstract void extract(Set<OWLOntology> ontologies) throws SQLException;
    
}
