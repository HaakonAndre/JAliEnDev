package alien.shell;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import utils.SystemProcess;
import alien.test.utils.Functions;

/**
 * @author ron
 * @since October 11, 2011
 */
public class ShellEditor {

	private static Map<String, String> availableEditors = null;

	private static String editor = null;

	private static Map<String, String> getAvailableEditors() {
		if (availableEditors != null)
			return availableEditors;

		final Map<String, String> temp = new HashMap<>();

		// in the order of preference, which is the first found will be used by
		// default
		for (final String ed : Arrays.asList("vim", "vi", "emacs", "mcedit", "joe", "pico")) {
			final String path = Functions.which(ed);

			if (path != null) {
				temp.put(ed, path);

				if (editor == null)
					editor = path;
			}
		}

		availableEditors = Collections.unmodifiableMap(temp);

		return availableEditors;
	}

	/**
	 * location of the a user set editor binary
	 */
	private static String userSetEditor = null;

	/**
	 * Set custom editor
	 * 
	 * @param editorPath
	 * @return previously set editor
	 */
	public static String setUserEditor(final String editorPath) {
		final String old = userSetEditor;

		userSetEditor = editorPath;

		return old;
	}

	/**
	 * @param localFile
	 * @return <code>true</code> if the file was successfully edited (command
	 *         was called, but we are not sure if the file was actually changed
	 *         or not)
	 */
	protected static boolean editFile(final String localFile) {
		getAvailableEditors();

		if (editor == null)
			// no editor available
			return false;

		if (System.console() == null)
			// cannot edit without an interactive console
			return false;

		final String command = editor + " " + localFile;

		final SystemProcess sp = new SystemProcess(command);

		final int exitStatus = sp.execute();

		return exitStatus == 0;
	}
}
