package alien.shell;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import jline.FileNameCompletor;

/**
 * @author ron
 * @since September 29, 2011
 */
public class GridLocalFileCompletor extends FileNameCompletor {

	private final BusyBox busy;

	/**
	 * @param box
	 */
	public GridLocalFileCompletor(BusyBox box) {
		busy = box;
	}

	public int complete(String buf, int cursor,
			@SuppressWarnings("rawtypes") final List candidates) {

		if (buf.contains("file://") && cursor >= buf.indexOf("file://"))
			return super.complete(buf.replace("file://", ""), cursor - 7,
					candidates) + 7;
		else
			return gridComplete(buf, cursor, candidates);
	}

	private int gridComplete(final String buf, final int cursor,
			@SuppressWarnings("rawtypes") final List candidates) {
		String buffer = (buf == null) ? "" : buf;

		String translated = buffer;

		// special character: ~ maps to the user's home directory
		if (translated.startsWith("~" + File.separator)) {
			translated = busy.getCurrentDir() + translated.substring(2);
		} else if (translated.startsWith("~")) {
			translated = busy.getCurrentDir();
		} else if (!(translated.startsWith(File.separator))) {
			translated = busy.getCurrentDir() // + File.separator
					+ translated;
		}

		final String dir;

		if (translated.endsWith(File.separator))
			dir = translated;
		else
			dir = translated.substring(0, translated.lastIndexOf('/'));
	
		final String listing = busy.callJAliEnGetString("ls -c " + dir);

		final StringTokenizer tk = new StringTokenizer(listing);
		List<String> entries = new ArrayList<String>();
		while (tk.hasMoreElements())
			entries.add((String) tk.nextElement());

		try {
			return gridMatchFiles(buffer, translated, entries, candidates);

		} finally {
			// we want to output a sorted list of files
			sortFileNames(candidates);
		}
	}

	@SuppressWarnings("unchecked")
	private int gridMatchFiles(String buffer, String translated,
			List<String> entries, @SuppressWarnings("rawtypes") List candidates) {

		if (entries == null) {
			return -1;
		}

		int matches = 0;

		// first pass: just count the matches
		for (String lfn : entries)
			if (lfn.startsWith(translated))
				matches++;

		for (String lfn : entries)
			if (lfn.startsWith(translated)) {
				String name = lfn.substring(lfn.lastIndexOf('/') + 1)
						+ (((matches == 1) && lfn.endsWith("/")) ? File.separator
								: " ");

				candidates.add(name);
			}

		final int index = buffer.lastIndexOf(File.separator);

		return index + File.separator.length();
	}
}
