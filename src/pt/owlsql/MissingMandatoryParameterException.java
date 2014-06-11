package pt.owlsql;

public class MissingMandatoryParameterException extends Exception {
    
    private static final long serialVersionUID = 2714102395837922083L;
    
    
    public MissingMandatoryParameterException() {}
    
    
    public MissingMandatoryParameterException(String message) {
        super(message);
    }
    
    
    public MissingMandatoryParameterException(Throwable cause) {
        super(cause);
    }
    
    
    public MissingMandatoryParameterException(String message, Throwable cause) {
        super(message, cause);
    }
}
