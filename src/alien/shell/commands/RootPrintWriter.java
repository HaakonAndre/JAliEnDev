package alien.shell.commands;

import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;

class RootPrintWriter extends UIPrintWriter{
	
	
//	# stream special characters according to CODEC.h
//	my $streamend       = chr 0;
//	my $fieldseparator  = chr 1;
//	my $fielddescriptor = chr 2;
//	my $columnseparator = chr 3;
//	my $stdoutindicator = chr 4;
//	my $stderrindicator = chr 5;
//	my $outputindicator = chr 6;
//	my $outputterminator = chr 7;
	
//	aliensh:[alice] [3] /alice/cern.ch/user/s/sschrein/tutorial/ >gbbox -d ls -la
//	===============>Stream stdout
//	  [Col 0]:    >drwxr-xr-x   sschrein sschrein            0 Feb 18 09:35    .                                 
//	<
//	  [Col 1]:    >drwxr-xr-x   sschrein admin               0 Aug 04 23:16    ..                                 
//	<
//	  [Col 2]:    >drwxr-xr-x   sschrein sschrein            0 Feb 18 09:37    .tutorial.jdl                                 
//	<
//	  [Col 3]:    >drwxr-xr-x   sschrein sschrein            0 Feb 18 09:42    .tutorial_textfile                                 
//	<
//	  [Col 4]:    >drwxr-xr-x   sschrein sschrein            0 Feb 18 10:26    .tut_validaton.sh                                 
//	<
//	  [Col 5]:    >-rwxr-xr-x   sschrein sschrein      2231480 Sep 08 10:42    alien_tutorial.pdf                                 
//	<
//	  [Col 6]:    >-rwxr-xr-x   sschrein sschrein          167 Feb 18 15:11    lala                    
//	  <
//	  [Col 14]:    >-rwxr-xr-x   sschrein sschrein         1690 Feb 18 10:26    tut_validaton.sh                                 
//	<
//	
//	===============>Stream stderr
//	  [Col 0]:    ><
//
//	===============>Stream result_structure
//	  [Col 0]:    [Tag: group] => >sschrein<
//	    [Tag: permissions] => >drwxr-xr-x<
//	    [Tag: date] => >Feb 18 09:35<
//	    [Tag: name] => >.<
//	    [Tag: user] => >sschrein<
//	    [Tag: path] => >/alice/cern.ch/user/s/sschrein/tutorial/<
//	    [Tag: md5] => ><
//	    [Tag: size] => >0<
//	  [Col 1]:    [Tag: group] => >admin<
//	    [Tag: permissions] => >drwxr-xr-x<
//	    [Tag: date] => >Aug 04 23:16<
//	    [Tag: name] => >..<
//	    [Tag: user] => >sschrein<
//	    [Tag: path] => >/alice/cern.ch/user/s/sschrein/tutorial/<
//	    [Tag: md5] => ><
//	    [Tag: size] => >0<
//	  [Col 2]:    [Tag: group] => >sschrein<
//	    [Tag: permissions] => >drwxr-xr-x<
//	    [Tag: date] => >Feb 18 09:37<
//	    [Tag: name] => >.tutorial.jdl<
//	    [Tag: user] => >sschrein<
//	    [Tag: path] => >/alice/cern.ch/user/s/sschrein/tutorial/<
//	    [Tag: md5] => ><
//	    [Tag: size] => >0<
//	  [Col 3]:    [Tag: group] => >sschrein<
//	    [Tag: permissions] => >drwxr-xr-x<
//	    [Tag: date] => >Feb 18 09:42<
//	    [Tag: name] => >.tutorial_textfile<
//	    [Tag: user] => >sschrein<
//	    [Tag: path] => >/alice/cern.ch/user/s/sschrein/tutorial/<
//	    [Tag: md5] => ><
//	    [Tag: size] => >0<
//
//	    ===============>Stream misc_hash
//	    [Col 0]:    [Tag: pwd] => >/alice/cern.ch/user/s/sschrein/tutorial/<
//
//	  aliensh:[alice] [4] /alice/cern.ch/user/s/sschrein/tutorial/ >

	
	
	

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils
			.getLogger(RootPrintWriter.class.getCanonicalName());

	private static final String errTag = "stderr:";

	private OutputStream os;

	RootPrintWriter(OutputStream os) {
		this.os = os;
	}

	private void print(String line) {
		try {
			os.write(line.getBytes());
		} catch (IOException e) {
			e.printStackTrace();
			logger.log(Level.FINE, "Could not write to OutputStream" + line, e);
		}
	}

	protected void printOutln(String line) {
		print(line + "\n");
	}

	protected void printErrln(String line) {
		print(errTag + line + "\n");
	}

	@Override
	protected void flush() {
		// TODO Auto-generated method stub
		
	}
}
