package pt.owlsql;

public class InvalidStringException extends Exception {
    
    private static final long serialVersionUID = 1443418155622427779L;
    
    
    public InvalidStringException(String message) {
        super(message);
    }
}
