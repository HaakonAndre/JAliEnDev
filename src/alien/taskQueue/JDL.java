package alien.taskQueue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lazyj.Format;
import lazyj.StringFactory;
import lazyj.Utils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.io.IOUtils;

/**
 * @author costing
 * 
 */
public class JDL implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4803377858842338873L;
	private final Map<String, Object> jdlContent = new LinkedHashMap<String, Object>();
	
	/**
	 * Empty constructor. The values can be populated with {@link #set(String, Object)}  and {@link #append(String, String)}
	 */
	public JDL(){
		// empty
	}
	
	/**
	 * A file in the catalogue
	 * 
	 * @param file
	 * @throws IOException
	 */
	public JDL(final LFN file) throws IOException {
		this(IOUtils.getContents(file));
	}
	
	/**
	 * A file in the catalogue
	 * 
	 * @param file
	 * @throws IOException
	 */
	public JDL(final GUID file) throws IOException {
		this(IOUtils.getContents(file));
	}

	/**
	 * a local file
	 * 
	 * @param file
	 * @throws IOException
	 */
	public JDL(final File file) throws IOException {
		this(Utils.readFile(file.getAbsolutePath()));
	}
	
	/**
	 * a job ID
	 * 
	 * @param jobID
	 * @throws IOException
	 */
	public JDL(final int jobID) throws IOException{
		this(Job.sanitizeJDL(TaskQueueUtils.getJDL(jobID)));
	}
	
	/**
	 * @param jdl
	 * @return jdl content stripped of comments
	 */
	public static final String removeComments(final String jdl){
		if (jdl==null || jdl.length()==0 || jdl.indexOf('#')<0)
			return jdl;
		
		final BufferedReader br = new BufferedReader(new StringReader(jdl));
		
		String line;
		
		final StringBuilder sb = new StringBuilder(jdl.length());
		
		try {
			while ( (line=br.readLine())!=null ){
				if (line.length()==0 || line.trim().startsWith("#"))
					continue;
				
				sb.append(line).append('\n');
			}
		}
		catch (IOException e) {
			// cannot be
		}
		
		return sb.toString();
	}

	/**
	 * the full contents
	 * 
	 * @param origContent
	 * @throws IOException
	 */
	public JDL(final String origContent) throws IOException {
		if (origContent == null || origContent.length() == 0) {
			throw new IOException("Content is " + (origContent == null ? "null" : "empty"));
		}

		int iPrevPos = 0;

		int idxEqual = -1;
		
		final String content = removeComments(origContent);

		while ((idxEqual = content.indexOf('=', iPrevPos + 1)) > 0) {
			final String sKey = clean(content.substring(iPrevPos, idxEqual).trim());

			int idxEnd = idxEqual + 1;

			boolean bEsc = false;
			boolean bQuote = false;

			outer: while (idxEnd < content.length()) {
				final char c = content.charAt(idxEnd);

				switch (c) {
				case '\\':
					bEsc = !bEsc;

					break;
				case '"':
					if (!bEsc)
						bQuote = !bQuote;

					bEsc = false;

					break;
				case ';':
					if (!bEsc && !bQuote)
						break outer;

					bEsc = false;

					break;
				default:
					bEsc = false;
				}

				idxEnd++;
			}

			final String sValue = content.substring(idxEqual + 1, idxEnd).trim();

			final Object value = parseValue(sValue);

			// System.err.println(sKey +" = "+value);

			if (value != null) {
				jdlContent.put(sKey, value);
			}

			iPrevPos = idxEnd + 1;
		}
	}

	private static String clean(final String input) {
		String output = input;

		while (output.startsWith("#")) {
			int idx = output.indexOf('\n');

			if (idx < 0)
				return StringFactory.get("");

			output = output.substring(idx + 1);
		}

		while (output.startsWith("\n"))
			output = output.substring(1);

		while (output.endsWith("\n"))
			output = output.substring(0, output.length() - 1);

		return StringFactory.get(output);
	}
	
	/**
	 * @return the set of keys present in the JDL
	 */
	public Set<String> keySet(){
		return Collections.unmodifiableSet(jdlContent.keySet());
	}
	
	/**
	 * Get the value of a key
	 * 
	 * @param key
	 * 
	 * @return the value, can be a String, a Number, a Collection ...
	 */
	public Object get(final String key) {
		for (final Map.Entry<String, Object> entry: jdlContent.entrySet()){
			if (entry.getKey().equalsIgnoreCase(key))
				return entry.getValue();
		}
		
		return null;
	}

	/**
	 * Get the value of this key as String
	 * 
	 * @param key
	 * 
	 * @return the single value if this was a String, the first entry of a
	 *         Collection (based on the iterator)...
	 */
	public String gets(final String key) {
		final Object o = get(key);

		return getString(o);
	}
	
	/**
	 * @param key
	 * @return the integer value, or <code>null</code> if the key is not defined or is not a number
	 */
	public Integer getInteger(final String key){
		final Object o = get(key);
		
		if (o==null)
			return null;
		
		if (o instanceof Number){
			return Integer.valueOf(((Number) o).intValue());
		}
		
		try{
			return Integer.valueOf(Integer.valueOf(getString(o)).intValue());
		}
		catch (NumberFormatException nfe){
			// ignore
		}
		
		return null;	// not an integer
	}
	
	/**
	 * @param key
	 * @return the float value, or <code>null</code> if the key is not defined or is not a number
	 */
	public Float getFloat(final String key){
		final Object o = get(key);
		
		if (o==null)
			return null;
		
		if (o instanceof Number){
			return Float.valueOf(((Number) o).floatValue());
		}
		
		try{
			return Float.valueOf(getString(o));
		}
		catch (NumberFormatException nfe){
			// ignore
		}
		
		return null;	// not an integer		
	}
	
	private static String getString(final Object o) {
		if (o == null)
			return null;

		if (o instanceof Collection<?>) {
			final Collection<?> c = (Collection<?>) o;

			if (c.size() > 0)
				return getString(c.iterator().next());
		}

		return o.toString();
	}
	
	private static final Object parseValue(final String value) {
		if (value.startsWith("\"") && value.endsWith("\"")) {
			return StringFactory.get(value.substring(1, value.length() - 1));
		}

		if (value.startsWith("{") && value.endsWith("}")) {
			return toList(value.substring(1, value.length() - 1));
		}

		try{
			return Integer.valueOf(value);
		}
		catch (NumberFormatException nfe){
			// ignore
		}
		
		try{
			return Double.valueOf(value);
		}
		catch (NumberFormatException nfe){
			// ignore
		}

		// signal that this is not a string in quotes
		return new StringBuilder(value);
	}

	private static final Pattern PANDA_RUN_NO = Pattern
			.compile(".*/run(\\d+)$");

	/**
	 * Get the run number if this job is a simulation job
	 * 
	 * @return run number
	 */
	public int getSimRun() {
		final String split = gets("splitarguments");

		if (split == null) {
			// is it a Panda production ?

			final String sOutputDir = getOutputDir();

			if (sOutputDir == null || sOutputDir.length() == 0)
				return -1;

			final Matcher m = PANDA_RUN_NO.matcher(sOutputDir);

			if (m.matches())
				return Integer.parseInt(m.group(1));

			return -1;
		}

		if (split.indexOf("sim") < 0) {
			return -1;
		}

		final StringTokenizer st = new StringTokenizer(split);

		while (st.hasMoreTokens()) {
			final String s = st.nextToken();

			if (s.equals("--run")) {
				if (st.hasMoreTokens()) {
					final String run = st.nextToken();

					try {
						return Integer.parseInt(run);
					} catch (NumberFormatException nfe) {
						return -1;
					}
				}

				return -1;
			}
		}

		return -1;
	}

	/**
	 * Get the number of jobs this masterjob will split into. Only works for
	 * productions that split in a fixed number of jobs.
	 * 
	 * @return the number of subjobs
	 */
	public int getSplitCount() {
		final String split = gets("split");

		if (split == null || split.length() == 0)
			return -1;

		if (split.startsWith("production:")) {
			try {
				return Integer
						.parseInt(split.substring(split.lastIndexOf('-') + 1));
			} catch (NumberFormatException nfe) {
				// ignore
			}
		}

		return -1;
	}

	/**
	 * Get the list of input files
	 * 
	 * @return the list of input files
	 */
	public List<String> getInputFiles() {
		return getInputFiles(true);
	}

	/**
	 * @return the input data
	 */
	public List<String> getInputData() {
		return getInputData(true);
	}

	/**
	 * Get the list of input data
	 * 
	 * @param bNodownloadIncluded
	 *            include or not the files with the ",nodownload" option
	 * 
	 * @return list of input data to the job
	 */
	public List<String> getInputData(final boolean bNodownloadIncluded) {
		return getInputList(bNodownloadIncluded, "InputData");
	}

	/**
	 * Get the list of input files
	 * 
	 * @param bNodownloadIncluded
	 *            flag to include/exclude the files for which ",nodownload" is
	 *            indicated in the JDL
	 * @return list of input files
	 */
	public List<String> getInputFiles(final boolean bNodownloadIncluded) {
		List<String> ret = getInputList(bNodownloadIncluded, "InputFile");

		if (ret == null)
			ret = getInputList(bNodownloadIncluded, "InputBox");

		return ret;
	}

	/**
	 * Get the list of output files
	 * 
	 * @return list of output files
	 */
	public List<String> getOutputFiles() {
		List<String> ret = getInputList(false, "Output");
		if (ret == null)
			ret = new LinkedList<String>();
		List<String> retf = getInputList(false, "OutputFile");
		if (retf != null)
			ret.addAll(retf);
		List<String> reta = getInputList(false, "OutputArchive");
		if (reta != null)
			ret.addAll(retf);

		return ret;
	}

	
	/**
	 * Get the list of arguments
	 * 
	 * @return list of arguments
	 */
	public List<String> getArguments() {
		return getInputList(false, "Arguments");
	}
	
	/**
	 * Get the user name of the job
	 * 
	 * @return user
	 */
	public String getUser() {
		return gets("User");
	}
	
	
	/**
	 * Get the executable
	 * 
	 * @return executable
	 */
	public String getExecutable() {
		return gets("Executable");
	}
	
	/**
	 * Get the output directory
	 * 
	 * @return output directory
	 */
	public String getOutputDirectory() {
		return gets("OutputDir");
	}
	
	/**
	 * Get the list of input files for a given tag
	 * 
	 * @param bNodownloadIncluded
	 *            flag to include/exclude the files for which ",nodownload" is
	 *            indicated in the JDL
	 * @param sTag
	 *            tag to extract the list from
	 * @return input list
	 */
	public List<String> getInputList(final boolean bNodownloadIncluded,
			final String sTag) {
		final Object o = get(sTag);

		if (o == null) {
			// System.err.println("No such tag: "+sTag);
			// for (Map.Entry<?, ?> me: jdlContent.entrySet()){
			// System.err.println("'"+me.getKey()+"' = '"+me.getValue()+"'");
			// }

			return null;
		}

		final List<String> ret = new LinkedList<String>();

		if (o instanceof CharSequence) {
			final String s = ((CharSequence) o).toString();

			if (bNodownloadIncluded || s.indexOf(",nodownload") < 0)
				ret.add(removeLF(s));

			return ret;
		}

		if (o instanceof Collection<?>) {
			final Iterator<?> it = ((Collection<?>) o).iterator();

			while (it.hasNext()) {
				final Object o2 = it.next();

				if (o2 instanceof String) {
					final String s = (String) o2;
					if (bNodownloadIncluded || s.indexOf(",nodownload") < 0)
						ret.add(removeLF(s));
				}
			}
		}

		return ret;
	}

	private static String removeLF(final String s) {
		String ret = s;

		if (ret.startsWith("LF:"))
			ret = ret.substring(3);

		int idx = ret.indexOf(",nodownload");

		if (idx >= 0)
			ret = ret.substring(0, idx);

		return ret;
	}

	private static List<String> toList(final String value) {
		final List<String> ret = new LinkedList<String>();

		int idx = value.indexOf('"');

		if (idx < 0) {
			ret.add(value);
			return ret;
		}

		do {
			int idx2 = value.indexOf('"', idx + 1);

			if (idx2 < 0)
				return ret;

			while (value.charAt(idx2 - 1) == '\'') {
				idx2 = value.indexOf('"', idx2 + 1);

				if (idx2 < 0)
					return ret;
			}

			ret.add(StringFactory.get(value.substring(idx + 1, idx2)));

			idx = value.indexOf('"', idx2 + 1);
		} while (idx > 0);

		return ret;
	}

	/**
	 * Get the job comment
	 * 
	 * @return job comment
	 */
	public String getComment() {
		final String sType = gets("Jobtag");

		if (sType == null)
			return null;

		if (sType.toLowerCase().startsWith("comment:"))
			return sType.substring(8).trim();

		return sType.trim();
	}
	
	/**
	 * Set the job comment
	 * 
	 * @param comment
	 */
	public void setComment(final String comment){
		final List<String> oldTag = getList("Jobtag");
		final List<String> newTag = new ArrayList<String>();
				
		if (oldTag!=null){
			for (final String s: oldTag){
				if (!s.startsWith("comment:"))
					newTag.add(s);
			}
			
			clear("Jobtag");
		}
		
		for (final String s: newTag){
			append("Jobtag", s);
		}

		if (comment!=null)
			append("Jobtag", "comment:"+comment);
	}

	/**
	 * Get the (package, version) mapping. Ex: { (AliRoot -> v4-19-16-AN), (ROOT
	 * -> v5-26-00b-6) }
	 * 
	 * @return packages
	 */
	@SuppressWarnings("unchecked")
	public Map<String, String> getPackages() {
		final Object o = get("Packages");

		if (!(o instanceof List))
			return null;

		final Iterator<String> it = ((List<String>) o).iterator();

		final Map<String, String> ret = new HashMap<String, String>();

		while (it.hasNext()) {
			final String s = it.next();

			try {
				int idx = s.indexOf('@');

				final int idx2 = s.indexOf("::", idx + 1);

				final String sPackage = s.substring(idx + 1, idx2);

				final String sVersion = s.substring(idx2 + 2);

				ret.put(sPackage, sVersion);
			} catch (Throwable t) {
				System.err
						.println("Exception parsing package definition: " + s);
			}
		}

		return ret;
	}
	
	/**
	 * @param key
	 * @return the list for this key
	 */
	@SuppressWarnings("unchecked")
	public List<String> getList(final String key){
		final Object o = get(key);
		
		if (o==null)
			return null;
		
		if (o instanceof List)
			return Collections.unmodifiableList((List<String>)o);
		
		if (o instanceof CharSequence)
			return Arrays.asList(o.toString());
		
		return null;		
	}
	
	/**
	 * Clear a list
	 * 
	 * @param key
	 */
	public void clear(final String key){
		final Object o = get(key);
		
		if (o==null)
			return;
		
		if (o instanceof List){
			((List<?>)o).clear();
			return;
		}
		
		set(key, new LinkedList<String>());
	}
	
	/**
	 * Get the output directory
	 * 
	 * @return output directory
	 */
	public String getOutputDir() {
		String s = gets("OutputDir");

		if (s == null)
			return null;

		int idx = s.indexOf("#alien");

		if (idx >= 0) {
			int idxEnd = s.indexOf("#", idx + 1);

			if (idxEnd > 0)
				s = s.substring(0, idx);
		}

		if (s.endsWith("/"))
			s = s.substring(0, s.length() - 1);

		return s;
	}

	/**
	 * Get the number of events/job in this simulation run
	 * 
	 * @return events/job, of -1 if not supported
	 */
	public int getSimFactor() {
		final List<String> inputFiles = getInputFiles();

		if (inputFiles == null)
			return -1;

		for (String file : inputFiles) {
			if (file.endsWith("sim.C"))
				return getSimFactor(LFNUtils.getLFN(file));
		}

		return -1;
	}

	// void sim(Int_t nev=300) {
	// void sim(Int_t nev = 300) {
	private static final Pattern pSimEvents = Pattern
			.compile(".*void.*sim.*\\s+n(\\_)?ev\\s*=\\s*(\\d+).*");

	/**
	 * Get the number of events/job that this macro is expected to produce
	 * 
	 * @param f
	 * @return events/job, or -1 if not supported
	 */
	public static int getSimFactor(final LFN f) {
		final GUID guid = GUIDUtils.getGUID(f);

		if (guid == null)
			return -1;
		
		final String sContent = IOUtils.getContents(guid);

		try {
			final BufferedReader br = new BufferedReader(new StringReader(
					sContent));

			String sLine;

			while ((sLine = br.readLine()) != null) {
				Matcher m = pSimEvents.matcher(sLine);

				if (m.matches())
					return Integer.parseInt(m.group(2));
			}
		} catch (IOException ioe) {
			// ignore, cannot happen
		}

		return -1;
	}
	
	private static final String tab = "        ";
	
	@Override
	public String toString(){
		final StringBuilder sb = new StringBuilder();
		
		for (final Map.Entry<String, Object> entry: jdlContent.entrySet()){
			if (sb.length()>0)
				sb.append('\n');
			
			sb.append(tab).append(entry.getKey()).append(" = ");
			
			append(sb, entry.getValue());
			
			sb.append(";\n");
		}
		
		return sb.toString();
	}
	
	private static final void append(final StringBuilder sb, final Object o){
		if (o instanceof StringBuilder || o instanceof StringBuffer || o instanceof Number){
			sb.append(o);
		}
		else
		if (o instanceof Collection){
			sb.append("{");
			
			final Collection<?> c = (Collection<?>) o;
			
			boolean first = true;
			
			for (final Object o2: c){
				if (!first)
					sb.append(",");
				else
					first = false;
				
				sb.append("\n").append(tab).append(tab).append("\"").append(o2).append("\"");
			}
			
			sb.append(tab).append("\n").append(tab).append("}");
		}
		else{
			sb.append('"').append(o.toString()).append('"');
		}
	}
	
	/**
	 * Delete a key
	 * 
	 * @param key
	 * @return the old value, if any 
	 */
	public Object delete(final String key){
		final Iterator<Map.Entry<String, Object>> it = jdlContent.entrySet().iterator();
		
		while (it.hasNext()){
			final Map.Entry<String, Object> entry = it.next();
			
			if (entry.getKey().equalsIgnoreCase(key)){
				it.remove();
				return entry.getValue();
			}
		}
		
		return null;
	}
	
	/**
	 * Set the value of a key. As value you can pass either:<br>
	 * <ul>
	 * <li>a String object, the value of which is to be put in quotes</li>
	 * <li>a StringBuilder object, then the content is set in the JDL without quotes (for example the Requirements field)</li>
	 * <li>a Collection, the values of which will be saved as an array of strings in the JDL</li>
	 * <li>a Number object, which will be saved without quotes</li>
	 * <li>any other Object, for which toString() will be called</li>
	 * </ul>
	 * 
	 * @param key JDL key name
	 * @param value (new) value
	 * @return the previously set value, if any
	 */
	public Object set(final String key, final Object value){
		if (value==null){
			return delete(key);
		}
		
		final Object old = get(key);

		Object newValue = value;
		
		if (newValue instanceof Collection){
			final List<String> localCopy = new LinkedList<String>();
			
			for (final Object o: (Collection<?>)newValue){
				localCopy.add(StringFactory.get(o.toString()));
			}
		}
		else
		if (newValue instanceof String){
			newValue = StringFactory.get((String) newValue);
		}
		
		if (old!=null){
			for (final Map.Entry<String, Object> entry: jdlContent.entrySet()){
				if (entry.getKey().equalsIgnoreCase(key)){
					entry.setValue(newValue);
					break;
				}
			}
		}
		else{
			jdlContent.put(key, newValue);
		}
		
		return old;
	}
	
	/**
	 * Append a String value to an array. If there is a previously set single value then it is transformed in an array and the previously set value is kept as the first entry
	 * of it. 
	 * 
	 * @param key
	 * @param value
	 */
	@SuppressWarnings("unchecked")
	public void append(final String key, final String value){
		if (key==null || value==null)
			return;
		
		final Object old = get(key);
		
		final Collection<String> values;
		
		if (old==null){
			values = new LinkedList<String>();
			jdlContent.put(key, values);
		}
		else
		if (old instanceof Collection){
			values = (Collection<String>) old;
		}
		else{
			values = new LinkedList<String>();
			values.add(old.toString());

			boolean added = false;
			
			for (final Map.Entry<String, Object> entry: jdlContent.entrySet()){
				if (entry.getKey().equalsIgnoreCase(key)){
					added = true;
					entry.setValue(values);
					break;
				}
			}
			
			if (!added)
				jdlContent.put(key, values);
		}
		
		values.add(StringFactory.get(value));
	}
	

	/**
	 * @param requirement extra constraint to add to the job
	 * @return <code>true</code> if this extra requirement was added
	 */
	public final boolean addRequirement(final String requirement){
		if (requirement==null || requirement.length()==0)
			return false;
		
		Object old = get("Requirements");
		
		final StringBuilder newValue;
		
		if (old!=null){
			if (old instanceof StringBuilder)
				newValue = (StringBuilder) old;
			else{
				newValue = new StringBuilder();
				newValue.append(getString(old));
				
				set("Requirements", newValue);
			}

			if (newValue.indexOf(requirement)>=0)
				return false;

			if (newValue.length()>0)
				newValue.append(" && ");
		}
		else{
			newValue = new StringBuilder();
			
			set("Requirements", newValue);
		}
	
		if (requirement.matches("^\\(.+\\)$"))
			newValue.append(requirement);
		else
			newValue.append("( ").append(requirement).append(" )");
		
		return true;
	}
	
	/**
	 * @return the HTML representation of this JDL
	 */
	public String toHTML(){
		final StringBuilder sb = new StringBuilder();
		
		for (final Map.Entry<String, Object> entry: jdlContent.entrySet()){
			if (sb.length()>0)
				sb.append("<br>");
			
			String key = entry.getKey();
			
			for (final String k: correctTags){
				if (k.equalsIgnoreCase(key)){
					key = k;
					break;
				}
			}
			
			sb.append("<B>").append(key).append("</B> = ");
			
			appendHTML(entry.getKey(), sb, entry.getValue());
			
			sb.append(";<BR>\n");
		}
		
		return sb.toString();
	}
	
	private static final void appendHTML(final String key, final StringBuilder sb, final Object o){
		if (o instanceof StringBuilder || o instanceof StringBuffer || o instanceof Number){
			if (o instanceof Number){
				sb.append("<font color=darkgreen>").append(o).append("</font>");
			}
			else
				sb.append(formatExpression(o.toString()));
		}
		else
		if (o instanceof Collection){
			sb.append("{<br><div style='padding-left:20px'>");
			
			final Collection<?> c = (Collection<?>) o;
			
			boolean first = true;
			
			for (final Object o2: c){
				if (!first)
					sb.append(",<br>");
				else
					first = false;
				
				String text = o2.toString();
				
				if (key.toLowerCase().startsWith("output") && !key.toLowerCase().equals("outputdir")){
					text = formatOutput(text);
				}
				else
				if (key.equalsIgnoreCase("packages")){
					text = formatPackages(text);
				}
				else
					text = "<font color=navy>"+Format.escHtml(text)+"</font>";
				
				sb.append('"').append(text).append('"');
			}
			
			sb.append("</div>}");
		}
		else{
			sb.append("\"<font color=navy>").append(o.toString()).append("</font>\"");
		}
	}
	
	private static final Pattern PACKAGES = Pattern.compile("^\\w+@\\w+::[a-zA-Z0-9._-]+$"); 
	private static final Pattern NUMBER = Pattern.compile("(?<=(\\s|^))\\d+(.(\\d+)?)?(E[+-]\\d+)?(?=(\\s|$))");
	private static final Pattern JDLFIELD = Pattern.compile("(?<=\\Wother\\.)[A-Z][a-zA-Z]+(?=\\W)");
	
	/**
	 * @param sLine
	 * @param p
	 * @param sPreffix
	 * @param sSuffix
	 * @return formatted pattern
	 */
	public static String highlightPattern(final String sLine, final Pattern p, final String sPreffix, final String sSuffix){
		final StringBuilder sb = new StringBuilder(sLine.length());
		
		final Matcher m = p.matcher(sLine);
		
		int iLastIndex = 0;
		
		while (m.find(iLastIndex)){
			final String sMatch = sLine.substring(m.start(), m.end());
	
			sb.append(sLine.substring(iLastIndex, m.start()));
			sb.append(Format.replace(sPreffix, "${MATCH}", sMatch));			
			sb.append(sMatch);
			sb.append(Format.replace(sSuffix, "${MATCH}", sMatch));
			
			iLastIndex = m.end();
		}
		
		sb.append(sLine.substring(iLastIndex));
		
		return sb.toString();
	}
	
	private static final String formatExpression(final String text){
		String arg = highlightPattern(text, NUMBER, "<font color=darkgreen>", "</font>");
		arg = highlightPattern(arg, JDLFIELD, "<I>", "</I>");
		
		final StringBuilder sb = new StringBuilder();
		
		int old = 0;
		int idx = arg.indexOf('"');
		
		while (idx>0){
			int idx2 = arg.indexOf('"', idx+1);
			
			if (idx2>idx){
				sb.append(arg.substring(old, idx+1));
				
				String stringValue = arg.substring(idx+1, idx2);
				
				if (PACKAGES.matcher(stringValue).matches())
					sb.append(formatPackages(stringValue));
				else
					sb.append("<font color=navy>").append(stringValue).append("</font>");
				
				sb.append('"');
				
				old = idx2+1;
				idx = arg.indexOf('"', old);
			}
			else
				break;
		}
		
		sb.append(arg.substring(old));
		
		return sb.toString();
	}
	
	private static final String formatPackages(final String arg){
		String text = arg;
		
		final StringBuilder sb = new StringBuilder();
		
		int idx = text.indexOf('@');
		
		if (idx>0){
			sb.append("<font color=#999900>").append(Format.escHtml(text.substring(0, idx))).append("</font>@");
			text = text.substring(idx+1);
		}
		
		idx = text.indexOf("::");
		
		if (idx>0){
			sb.append("<font color=green>").append(Format.escHtml(text.substring(0, idx))).append("</font>::<font color=red>").append(Format.escHtml(text.substring(idx+2))).append("</font>");
		}
		else
			sb.append(Format.escHtml(text));
		
		return sb.toString();
	}
	
	private static final String formatOutput(final String arg){
		String text = arg;
		
		final StringBuilder sb = new StringBuilder();
		
		final int idx = text.indexOf(':');
		
		int idx2 = text.indexOf('@'); 
		
		if (idx>0 && (idx2<0 || idx<idx2)){
			sb.append("<font color=red>").append(Format.escHtml(text.substring(0, idx))).append("</font>:");
			text = text.substring(idx+1);
			
			idx2 = text.indexOf('@');
		}
		
		if (idx2>=0){
			sb.append(Format.escHtml(text.substring(0, idx2+1))).append("<font color=green>").append(Format.escHtml(text.substring(idx2+1))).append("</font>");
		}
		else
			sb.append(Format.escHtml(text));
		
		return sb.toString();
	}
	
	private static final List<String> correctTags = Arrays.asList(
		"Arguments",
		"Executable",
		"GUIDFile",
		"InputBox", "InputDataList", "InputDataListFormat", "InputDownload", "InputFile",
		"JDLArguments", "JDLPath", "JDLProcessor", "JDLVariables", "JobLogOnClusterMonitor", "JobTag",
		"LPMActivity",
		"MasterJobID", "MemorySize", 
		"OrigRequirements", "Output", "OutputArchive", "OutputDir", "OutputFile",
		"Packages", "Price",
		"Requirements", 
		"SuccessfullyBookedPFNs",
		"TTL", "Type",
		"User",
		"ValidationCommand", 
		"WorkDirectorySize" 
	);
}
