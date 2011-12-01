package alien.log;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Level;

public class InfoFileHandler extends FileHandler {

	public InfoFileHandler() throws IOException, SecurityException {
		super();
		// TODO Auto-generated constructor stub
	}

	@Override
	public synchronized void setLevel(Level newLevel) throws SecurityException {
		// TODO Auto-generated method stub
		super.setLevel(Level.INFO);
	}
}
