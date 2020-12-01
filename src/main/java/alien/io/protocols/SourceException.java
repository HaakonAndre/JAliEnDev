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
	 * Status code of the exception
	 */
	private SourceExceptionCode code;

	/**
	 * @param reason
	 */
	public SourceException(SourceExceptionCode code, final String reason) {
		super(reason);
		this.code = code;
	}

	/**
	 * @param reason
	 * @param cause
	 */
	public SourceException(SourceExceptionCode code, final String reason, final Throwable cause) {
		super(reason, cause);
		this.code = code;
	}

	public SourceExceptionCode getCode() {
		return code;
	}
}
