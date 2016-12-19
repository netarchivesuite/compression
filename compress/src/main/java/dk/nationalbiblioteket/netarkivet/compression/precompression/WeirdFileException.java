package dk.nationalbiblioteket.netarkivet.compression.precompression;

/**
 * Checked exception representing any file that can't be compressed for some reason - e.g. not a valid warc file
 */
public class WeirdFileException extends Exception {

    public WeirdFileException(String message) {
        super(message);
    }

    public WeirdFileException(String message, Throwable cause) {
        super(message, cause);
    }
}
