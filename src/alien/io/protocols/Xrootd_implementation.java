package alien.io.protocols;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;


import alien.catalogue.access.XrootDEnvelope;

public class Xrootd_implementation {

	private static String xrdcpenvironment[] = { "LD_LIBRARY_PATH=/lib:/lib::/opt/alien/api/lib/" };
	private static String xrdcplocation = "/opt/alien/api/bin/xrdcpapmon";
	private static String xrdstatlocation = "/opt/alien/api/bin/xrdstat";
	private static String xrdcpdebug = " -d 3 ";

	private int statRetries = 3;
	private int statRetryTimes[] = { 6, 12, 30 };
	private int statRetryCounter = 0;

	private static String connectSpec = " -DIFirstConnectMaxCnt 6 ";
	private static String signaturePrexix = " -OD"; 
	private static String authzPrefix = " -OD&authz=\"";
	private static String authzSuffix = "\" ";

	private static String xrdcpcall = xrdcplocation + xrdcpdebug + connectSpec;
	private static String xrdstatcall = xrdstatlocation + xrdcpdebug
			+ connectSpec;

	private String stdout;
	private String stderr;
	private int rc;

	private void call(String execute) {

		System.out.println("We will execute: " + execute);
		stdout = null;
		stderr = null;
		rc = -1;

		try {

			Runtime rt = Runtime.getRuntime();
			Process pr = rt.exec(execute, xrdcpenvironment);
			BufferedReader input = new BufferedReader(new InputStreamReader(
					pr.getInputStream()));

			while ((stdout = input.readLine()) != null) {
				System.out.println(stdout);
			}

			BufferedReader error = new BufferedReader(new InputStreamReader(
					pr.getErrorStream()));

			while ((stderr = error.readLine()) != null) {
				System.out.println(stderr);
			}

			rc = pr.waitFor();
			System.out.println("Exited with error code " + rc);

		} catch (Exception e) {
			System.out.println(e.toString());
			e.printStackTrace();
		}
	}

	private long checkOutputOnSize(String pattern) {

		long size = 0;
		String line = null;
		BufferedReader reader = new BufferedReader(new StringReader(stdout));
		
		try {
			while ((line = reader.readLine()) != null) {
				// $doxrcheck =~ /Size:\ (\d+)/;
				if (line.length() > 0)
					System.out.println(line.charAt(0));
				if (line.startsWith("Size: ")) {
					String[] elements = line.split(" ");
					size = Long.parseLong(elements[1].trim());
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		return size;
	}

	private long getLocalFileSize(URL url) throws Exception {
		long size = -1;
		try {
			URLConnection conn = url.openConnection();
			size = conn.getContentLength();
			conn.getInputStream().close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return size;
	}

	public String returnStdOut() {
		return stdout;
	}

	public String returnStdErr() {
		return stderr;
	}

	public boolean xrdGet(String turl, String signedEnvelope, String localCopy,
			long size) {

		// String authz;
		// if(envelope.useoldenvelope) {
		// authz = envelope.getEncryptedEnvelope();
		// } else {
		// authz = envelope.getSignedEnvelope();
		// }

		// String xrdcpurl = envelope.getTurl();

		// $p=~ s/#.*$//;

		call(xrdcpcall + turl + " " + localCopy + signaturePrexix
				+ signedEnvelope);
		try {
			if (getLocalFileSize(new URL(localCopy)) == size)
				return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;

	}

	public boolean xrdUpload(String turl, String signedEnvelope,
			String localFile, int size) {

		call(xrdcpcall + " -np -v " + localFile + " -f " + turl + " " + signaturePrexix
				+ signedEnvelope);

		if (rc == 0)
			return true;
			// return oldXrdStat(turl, signedEnvelope, size);
		return false;

	}

	public boolean oldXrdStat(String turl, String signedEnvelope, int size) {

		call(xrdstatcall + turl);
		if (rc != 0) {
			if (statRetryCounter < statRetries) {
				try {
					Thread.sleep(statRetryTimes[statRetryCounter]);
				} catch (InterruptedException e) {
					// the VM doesn't want us to sleep anymore,
					// so get back to work
				}
				statRetryCounter++;
				return oldXrdStat(turl, signedEnvelope, size);
			} else {
				return false; // seems like connection problems ?!
			}

		} else {
			if (size == checkOutputOnSize(stdout))
				return true;
		}
		return false;
	}

}

// sub remove {
// my $self=shift;
// $self->debug(1,"Trying to remove the file $self->{PARSED}->{ORIG_PFN}");
// print "We are in the remove\n";
//
// # missing utilization of $ENV{ALIEN_XRDCP_TURL} +
// $ENV{ALIEN_XRDCP_SIGNED_ENVELOPE} / $ENV{ALIEN_XRDCP_ENVELOPE} !!!!!!
//
//
// # open(FILE, "| xrd $self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}
// my $pid = open2(*Reader, *Writer,
// "xrd $self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}" )
// or $self->info("Error calling xrd!") && return;
// print "Open\n";
// print Writer "rm $self->{PARSED}->{PATH}\n";
// print "Just wrote\n";
// my $error=close Writer;
// print "Reading\n";
// my $got="";
// my $oldAlarm=$SIG{ALRM};
// $SIG{ALRM}=\&_timeout;
// eval {
// alarm(5);
// while(my $l=<Reader>){
// print "Hello '$l'\n";
// $got.="$l";
// $l=~ /^\s*$/ and last;
// $l=~ /^\s*root:\/\// and last;
// }
// };
// my $error3=$@;
// alarm(0);
// $oldAlarm and $SIG{ALRM}=$oldAlarm;
// print "read (with error $error3)\n";
// my $error2=close Reader;
// print "Hello $error and $got and ($error2)\n";
// # my
// $command="xrm root://$self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}/$self->{PARSED}->{PATH}";
// # my $error=$self->_execute($command);#
// #
// # ($error<0) and return;
// $self->debug(1,"YUUHUUUUU!!\n");
// return
// "root://$self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}/$self->{PARSED}->{PATH}";
//
// }
//
// public String stat () {
// my $self=shift;
//
// $self->info("Getting the stat of $self->{PARSED}->{ORIG_PFN}");
// open (FILE, " xrdstat $self->{PARSED}->{ORIG_PFN}|") or
// $self->info("Error doing xrdstat") and return;
// my $buffer=join("", <FILE>);
// close FILE;
// $self->debug(1,"Got $buffer");
// return $buffer;
// }
// //
// //
// //
// // sub stage {
// // my $self=shift;
// // my $pfn=shift;
// // $self->info("WE HAVE TO STAGE AN XROOTD FILE ($pfn)!!");
// // system("xrdstage", $pfn);
// // return 1;
// // }
// //
// // sub isStaged{
// // my $self=shift;
// // my $pfn=shift;
// // $self->info("Checking if the file is in the xrootd cache");
// // system("xrdisonline", $pfn);
// return 1;
// }
// return 1;
// }
