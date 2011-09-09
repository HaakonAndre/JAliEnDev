package alien.shell.commands;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;

/**
 * @author ron
 * 
 */
public class RootPrintWriter extends UIPrintWriter {

	public static String streamend = String.valueOf((char) 0);
	public static String fieldseparator = String.valueOf((char) 1);
	public static String fielddescriptor = String.valueOf((char) 2);
	public static String columnseparator = String.valueOf((char) 3);
	public static String stdoutindicator = String.valueOf((char) 4);
	public static String stderrindicator = String.valueOf((char) 5);
	public static String outputindicator = String.valueOf((char) 6);
	public static String outputterminator = String.valueOf((char) 7);

	private String clientenv = "";

	private ArrayList<String> stdout = new ArrayList<String>();
	private ArrayList<String> stderr = new ArrayList<String>();

	private String args = "";

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils
			.getLogger(RootPrintWriter.class.getCanonicalName());

	private OutputStream os;

	RootPrintWriter(OutputStream os) {
		this.os = os;
	}

	protected void printOutln(String line) {
		stdout.add(line + "\n");
		System.out.println(line);
	}

	protected void printErrln(String line) {
		stderr.add(line + "\n");
	}

	protected void setenv(String cDir, String user, String cDirtiled) {
		clientenv = cDir;
	}

	protected boolean isRootPrinter() {
		return true;
	}

	protected void setReturnArgs(String args) {
		this.args = args;
	}

	private void print(String line) {
		try {
			os.write(line.getBytes());

		} catch (IOException e) {
			e.printStackTrace();
			logger.log(Level.FINE, "Could not write to OutputStream", e);
		}
	}

	protected void flush() {
		try {
			String sDebug = printDebug();

			System.out.println(sDebug);

			// os.write((sDebug + ((char) 0)).getBytes());
			os.write(stdoutindicator.getBytes());

			if (stdout.size() > 0) {
				for (String out : stdout)
					os.write((columnseparator + fieldseparator + out)
							.getBytes());
			} else 
				os.write((columnseparator + fieldseparator).getBytes());

			os.write(stderrindicator.getBytes());

			if (stderr.size() > 0) {
				for (String err : stderr)
					os.write((columnseparator + fieldseparator + err)
							.getBytes());
			} else
				os.write((columnseparator + fieldseparator).getBytes());

			os.write((outputindicator + args + outputterminator
					+ columnseparator + fielddescriptor + "pwd"
					+ fieldseparator + clientenv + streamend).getBytes());
			os.flush();

		} catch (IOException e) {
			e.printStackTrace();
			logger.log(Level.FINE, "Could not write to OutputStream", e);
		}
	}

	// testcode end

	private String printDebug() {

		String debug = stdoutindicator;
		if (stdout.size() > 0) {
			for (String out : stdout)
				debug += columnseparator + fieldseparator + out;
		} else
			debug += columnseparator + fieldseparator;

		debug += stderrindicator;
		if (stderr.size() > 0) {
			for (String err : stderr)
				debug += columnseparator + fieldseparator + err;
		} else
			debug += columnseparator + fieldseparator;

		debug += outputindicator + args + outputterminator + columnseparator
				+ fielddescriptor + "pwd" + fieldseparator + clientenv
				+ streamend;

		return testMakeTagsVisible(debug);

	}

	public static String testMakeTagsVisible(final String line) {
		String line1;

		line1 = line.replace(streamend, "<<STREAMEND>>");
		line1 = line1.replace(fieldseparator, "<<FDS>>");
		line1 = line1.replace(fielddescriptor, "\n<<FDD>>");
		line1 = line1.replace(columnseparator, "\n<<COL>>");
		line1 = line1.replace(stdoutindicator, "<<STDOUTIN>>");
		line1 = line1.replace(stderrindicator, "<<STDERRIN>>");
		line1 = line1.replace(outputindicator, "<<OUTPUTIN>>");
		line1 = line1.replace(outputterminator, "<<OUTPUTEND>>");

		return line1;
	}
	// testcode end

	//
	// my $calledfunction = shift @args;
	// if ($calledfunction eq "AliEn::Catalogue") {
	// $Data::Dumper::Indent = 0;
	// $returnargstream .= $columnseparator;
	// $returnargstream .= $fielddescriptor;
	// $returnargstream .= "__Dumper__";
	// $returnargstream .= $fieldseparator;
	// $returnargstream .= Dumper($result);
	// } else {
	// my $type = ref($result);
	//
	// if ( $type eq "HASH" ) {
	// $returnargstream .= $columnseparator;
	// foreach ( keys %$result ) {
	// $returnargstream .= $fielddescriptor;
	// $returnargstream .= $_;
	// $returnargstream .= $fieldseparator;
	// $returnargstream .= $result->{$_};
	//
	// }
	// }
	//
	// if ( ($type eq "SCALAR") || ($type eq "") ) {
	// my $cscalar = \$result;
	// if ( ref($cscalar) eq "SCALAR" ) {
	// $returnargstream .= $columnseparator;
	// $returnargstream .= $fielddescriptor;
	// $returnargstream .= "__result__";
	// $returnargstream .= $fieldseparator;
	// $returnargstream .= $result;
	// }
	// if ( ref($cscalar) eq "ARRAY" ) {
	// foreach (@$cscalar) {
	// $returnargstream .= $columnseparator;
	// $returnargstream .= $fielddescriptor;
	// $returnargstream .= "__result__";
	// $returnargstream .= $fieldseparator;
	// $returnargstream .= $_;
	// }
	// }
	// }
	//
	// if ( $type eq "ARRAY") {
	// my $larray;
	// foreach $larray ( @$result ) {
	// $returnargstream .= $columnseparator;
	// if ( ref($larray) eq "HASH" ) {
	// my $lkeys;
	// foreach $lkeys ( keys %$larray ) {
	// $returnargstream .= $fielddescriptor;
	// $returnargstream .= $lkeys;
	// $returnargstream .= $fieldseparator;
	// $returnargstream .= $larray->{$lkeys};
	//
	// }
	// }
	// if ( (ref($larray) eq "SCALAR") || (ref($larray) eq "") ){
	// $returnargstream .= $fielddescriptor;
	// $returnargstream .= "__result__";
	// $returnargstream .= $fieldseparator;
	// $returnargstream .= $larray;
	// }
	// }
	// }
	//
	//

	// # stream special characters according to CODEC.h
	// my $streamend = chr 0;
	// my $fieldseparator = chr 1;
	// my $fielddescriptor = chr 2;
	// my $columnseparator = chr 3;
	// my $stdoutindicator = chr 4;
	// my $stderrindicator = chr 5;
	// my $outputindicator = chr 6;
	// my $outputterminator = chr 7;

	// aliensh:[alice] [3] /alice/cern.ch/user/s/sschrein/tutorial/ >gbbox -d ls
	// -la
	// ===============>Stream stdout
	// [Col 0]: >drwxr-xr-x sschrein sschrein 0 Feb 18 09:35 .
	// <
	// [Col 1]: >drwxr-xr-x sschrein admin 0 Aug 04 23:16 ..
	// <
	// [Col 2]: >drwxr-xr-x sschrein sschrein 0 Feb 18 09:37 .tutorial.jdl
	// <
	// [Col 3]: >drwxr-xr-x sschrein sschrein 0 Feb 18 09:42 .tutorial_textfile
	// <
	// [Col 4]: >drwxr-xr-x sschrein sschrein 0 Feb 18 10:26 .tut_validaton.sh
	// <
	// [Col 5]: >-rwxr-xr-x sschrein sschrein 2231480 Sep 08 10:42
	// alien_tutorial.pdf
	// <
	// [Col 6]: >-rwxr-xr-x sschrein sschrein 167 Feb 18 15:11 lala
	// <
	// [Col 14]: >-rwxr-xr-x sschrein sschrein 1690 Feb 18 10:26
	// tut_validaton.sh
	// <
	//
	// ===============>Stream stderr
	// [Col 0]: ><
	//
	// ===============>Stream result_structure
	// [Col 0]: [Tag: group] => >sschrein<
	// [Tag: permissions] => >drwxr-xr-x<
	// [Tag: date] => >Feb 18 09:35<
	// [Tag: name] => >.<
	// [Tag: user] => >sschrein<
	// [Tag: path] => >/alice/cern.ch/user/s/sschrein/tutorial/<
	// [Tag: md5] => ><
	// [Tag: size] => >0<
	// [Col 1]: [Tag: group] => >admin<
	// [Tag: permissions] => >drwxr-xr-x<
	// [Tag: date] => >Aug 04 23:16<
	// [Tag: name] => >..<
	// [Tag: user] => >sschrein<
	// [Tag: path] => >/alice/cern.ch/user/s/sschrein/tutorial/<
	// [Tag: md5] => ><
	// [Tag: size] => >0<
	// [Col 2]: [Tag: group] => >sschrein<
	// [Tag: permissions] => >drwxr-xr-x<
	// [Tag: date] => >Feb 18 09:37<
	// [Tag: name] => >.tutorial.jdl<
	// [Tag: user] => >sschrein<
	// [Tag: path] => >/alice/cern.ch/user/s/sschrein/tutorial/<
	// [Tag: md5] => ><
	// [Tag: size] => >0<
	// [Col 3]: [Tag: group] => >sschrein<
	// [Tag: permissions] => >drwxr-xr-x<
	// [Tag: date] => >Feb 18 09:42<
	// [Tag: name] => >.tutorial_textfile<
	// [Tag: user] => >sschrein<
	// [Tag: path] => >/alice/cern.ch/user/s/sschrein/tutorial/<
	// [Tag: md5] => ><
	// [Tag: size] => >0<
	//
	// ===============>Stream misc_hash
	// [Col 0]: [Tag: pwd] => >/alice/cern.ch/user/s/sschrein/tutorial/<
	//
	// aliensh:[alice] [4] /alice/cern.ch/user/s/sschrein/tutorial/ >

}
