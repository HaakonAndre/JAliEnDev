package alien.shell.commands;

import java.util.ArrayList;
import java.util.Collection;

import joptsimple.OptionException;

/**
 * @author ron
 * @since Oct 29, 2011
 */
public class JAliEnCommandException extends OptionException{

	/**
	 * 
	 */
	private static final long serialVersionUID = -2175875716474305315L;

	/**
	 * @param options
	 */
	protected JAliEnCommandException(Collection<String> options) {
		super(options);
	}
	
	/**
	 */
	protected JAliEnCommandException() {
		super(new ArrayList<String>());
	}


}
