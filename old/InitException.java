package pt.owlsql;

public class InitException extends Exception {
    
    private static final long serialVersionUID = 708568497048512177L;
    
    
    public InitException() {}
    
    
    public InitException(String message) {
        super(message);
    }
    
    
    public InitException(Throwable cause) {
        super(cause);
    }
    
    
    public InitException(String message, Throwable cause) {
        super(message, cause);
    }
    
}
