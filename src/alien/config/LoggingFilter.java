package alien.config;

import java.util.logging.Filter;
import java.util.logging.LogRecord;

/**
 * @author costing
 *
 */
public class LoggingFilter implements Filter {

	@Override
	public boolean isLoggable(final LogRecord record) {
		final Object o = Context.getTheadContext("logger");
		
		if (o!=null){
			if (o instanceof StringBuilder){
				final StringBuilder sb = (StringBuilder) o;
				
				sb.append(record.toString());
			}
			else
			if (o instanceof SOAPLogger){
				final SOAPLogger logger = (SOAPLogger) o;
				
				logger.log(record);
			}
		}
		
		return true;
	}

}
