package pt.owlsql;

import java.sql.SQLException;


public abstract class Cacher extends Extractor {
    
    protected abstract void cache() throws SQLException;
    
}
