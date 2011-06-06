package alien.shell.commands;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import jline.FileNameCompletor;
import alien.api.catalogue.CatalogueApiUtils;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;

/**
 * @author ron
 * @since June 4, 2011
 */
public class GridFileCompletor extends FileNameCompletor {

	/**
	 * file separator
	 */
	public static final String separator = "/";

	public int complete(final String buf, final int cursor,
			@SuppressWarnings("rawtypes") final List candidates) {
		String buffer = (buf == null) ? "" : buf;

		// System.out.println("buffer2: " + buffer2 );
		//
		// char[] cbuf = buffer.toCharArray();
		//
		// int begin = cursor-1;
		// while(begin>=0 && cbuf[begin]!=' ')
		// begin--;
		//
		// int end = cursor;
		// while(end<cbuf.length && cbuf[end]!=' ')
		// end++;
		//
		// buffer = buffer.substring(begin,cursor);
		// System.out.println("buffer now: " + buffer );

		String translated = buffer.substring(buffer.lastIndexOf(' ') + 1);

		// // special character: ~ maps to the user's home directory
		// if (translated.startsWith("~" + separator)) {
		// translated = userhome
		// + translated.substring(1);
		// } else if (translated.startsWith("~")) {
		// translated =
		// CatalogueApiUtils.getLFN(UsersHelper.getHomeDir(JAliEnCOMMander.user.getName())).getParentDir()
		// .getCanonicalName();
		// } else if (!(translated.startsWith(separator))) {
		// // translated = new LFN("").getCanonicalName() + separator
		// // + translated;
		// translated = separator
		// + translated;
		// }
		translated = FileSystemUtils.getAbsolutePath(
				JAliEnCOMMander.user.getName(),
				JAliEnCOMMander.getCurrentDir().getCanonicalName(), translated);
		LFN lfn = CatalogueApiUtils.getLFN(translated);

		final LFN dir;

		if (translated.endsWith(separator)) {
			dir = lfn;
		} else {
			dir = CatalogueApiUtils.getLFN(translated.substring(0,
					translated.lastIndexOf('/')));
		}

		final LFN[] entries = (dir == null) ? new LFN[0]
				: directorylistLFNs(dir);

		try {
			System.out.println("b,t,e,c:" + buffer + "," + translated + ","
					+ entries + "," + candidates);
			return matchLFNs(buffer, cursor, translated, entries, candidates);
		} finally {
			// we want to output a sorted list of LFNs
			sortLFNNames(candidates);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected void sortLFNNames(final List LFNNames) {
		Collections.sort(LFNNames);
	}

	/**
	 * Match the specified <i>buffer</i> to the array of <i>entries</i> and
	 * enter the matches into the list of <i>candidates</i>. This method can be
	 * overridden in a subclass that wants to do more sophisticated file name
	 * completion.
	 * 
	 * @param buffer
	 *            the untranslated buffer
	 * @param cursor
	 * @param translated
	 *            the buffer with common characters replaced
	 * @param entries
	 *            the list of LFNs to match
	 * @param candidates
	 *            the list of candidates to populate
	 * 
	 * @return the offset of the match
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public int matchLFNs(String buffer, int cursor, String translated,
			LFN[] entries, List candidates) {

		if (entries == null) {
			return -1;
		}

		int matches = 0;

		// first pass: just count the matches
		for (int i = 0; i < entries.length; i++) {
			if (entries[i].getCanonicalName().startsWith(translated)) {
				matches++;
			}
		}

		// green - executable
		// blue - directory
		// red - compressed
		// cyan - symlink
		for (int i = 0; i < entries.length; i++) {
			if (entries[i].getCanonicalName().startsWith(translated)) {
				String name = entries[i].getName()
						+ (((matches == 1) && entries[i].isDirectory()) ? separator
								: " ");

				/*
				 * if (entries [i].isDirectory ()) { name = new ANSIBuffer
				 * ().blue (name).toString (); }
				 */
				candidates.add(name);
			}
		}

		final int index = buffer.lastIndexOf(separator);

		System.out.println("buffer:" + buffer);
		System.out.println("translated:" + translated);
		System.out.println("index:" + index);

		return index + separator.length();
	}

	private LFN[] directorylistLFNs(LFN lfn) {
		ArrayList<String> args = new ArrayList<String>();
		args.add(lfn.getCanonicalName());
		JAliEnCommandls ls = null;
		try {
			ls = (JAliEnCommandls) JAliEnCOMMander.getCommand("ls",
					new Object[] { args });
		} catch (Exception e) {
		}
		ls.silent();
		ls.execute();
		return (LFN[]) ls.getDirectoryListing().toArray(new LFN[] {});
	}

}
