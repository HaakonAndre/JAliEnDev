package alien.log;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Filter;
import java.util.logging.Level;
import java.util.logging.LogRecord;

/**
 * @author Alina Grigoras
 * sending the FINE log to a specific file
 */
public class WarningFileHandler extends FileHandler {

	
	/**
	 * Creates a simple FileHandler
	 * On this handler we change WARNING level and filter the output to the chosen level
	 * @throws IOException
	 * @throws SecurityException
	 */
	public WarningFileHandler() throws IOException, SecurityException {
		super();
		// TODO Auto-generated constructor stub
	}

	@Override
	public synchronized void setLevel(Level newLevel) throws SecurityException {
		// TODO Auto-generated method stub
		super.setLevel(Level.WARNING);
	}

	@Override
	public void setFilter(Filter newFilter) throws SecurityException {
		// TODO Auto-generated method stub
		super.setFilter(new Filter() {

			@Override
			public boolean isLoggable(LogRecord record) {
				// TODO Auto-generated method stub
				if(record.getLevel() != Level.WARNING)
					return false;
				return true;
			}
		});
	}
}

