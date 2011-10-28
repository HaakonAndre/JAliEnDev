package alien.shell;


/**
 * @author ron
 * @since Oct 27, 2011
 */
public class ShellColor {

	private static final String prefix = "\033[";

	private static final String sep = ";";

	private static final String suffix = "m";


	
	/**
	 * @return color code
	 */
	public static String reset(){
		return genTag(ForeColor.NONE, BackColor.NONE, Style.CLEAR);
	}
	
	
	/**
	 * @return color code
	 */
	public static String errorMessage(){
		return boldRed();
	}
	
	/**
	 * @return color code
	 */
	public static String boldRed(){
		return genTag(ForeColor.RED, BackColor.NONE, Style.BOLD);
	}
	
	/**
	 * @return color code
	 */
	public static String blue(){
		return genTag(ForeColor.BLUE, BackColor.NONE, Style.NONE);
	}
	
	/**
	 * @return color code
	 */
	public static String black(){
		return genTag(ForeColor.BLACK, BackColor.NONE, Style.NONE);
	}
	
	/**
	 * @return color code
	 */
	public static String boldBlack(){
		return genTag(ForeColor.BLACK, BackColor.NONE, Style.BOLD);
	}
	
	private static String genTag(final ForeColor fc, final BackColor bc, final Style st) {
		return prefix + fc.getCode() + sep + bc.getCode() + sep + st.getCode() + suffix;
	}


	private enum ForeColor {

		BLACK("30"), RED("31"), GREEN("32"), YELLOW("33"), BLUE("34"), MAGENTA(
				"35"), CYAN("36"), WHITE("37"), NONE("");

		private final String code;
		
		ForeColor(String code) {
			this.code = code;
		}

		protected String getCode() {
			return this.code;
		}

	}

	private enum BackColor {

		BLACK("40"), RED("41"), GREEN("42"), YELLOW("43"), BLUE("44"), MAGENTA(
				"45"), CYAN("46"), WHITE("47"), NONE("");

		private final String code;
		
		BackColor(String code) {
			this.code = code;
		}

		protected String getCode() {
			return this.code;
		}

	}

	private enum Style {

		CLEAR("0"), BOLD("1"), LIGHT("1"), DARK("2"), UNDERLINE("4"), REVERSE(
				"7"), HIDDEN("8"), NONE("");

		private final String code;
		
		Style(String code) {
			this.code = code;
		}

		protected String getCode() {
			return this.code;
		}

	}

}
