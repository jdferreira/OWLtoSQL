package pt.owlsql;


public class ConfigException extends Exception {
    
    private static final long serialVersionUID = -7086118365129378806L;
    
    
    public ConfigException(String message) {
        super(message);
    }
    
    
    public ConfigException(Throwable e) {
        super(e);
    }
}
