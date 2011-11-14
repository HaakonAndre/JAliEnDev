package alien.io.protocols;

import java.io.IOException;

/**
 * @author costing
 *
 */
public class SourceException extends IOException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5073609846381630091L;

	/**
	 * @param reason
	 */
	public SourceException(final String reason){
		super(reason);
	}
	
}
