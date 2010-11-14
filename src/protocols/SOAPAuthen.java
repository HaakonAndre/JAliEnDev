package protocols;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lia.util.UUID;

import alien.services.AuthenServer;

import alien.catalogue.access.AuthorizationFactory;
import alien.catalogue.access.CatalogueAccess;
import alien.catalogue.access.XrootDEnvelope;

/**
 * @author ron
 * @since Nov 12, 2010
 */
public class SOAPAuthen {

	
	private static final Pattern writeRequest = Pattern.compile("^write");
	private static final Pattern isPerlOption = Pattern.compile("^-");
	private static final Pattern isNumeric = Pattern.compile("^[0-9]+$");
	private static final Pattern SE_NAME = Pattern.compile("^[0-9a-zA-Z_\\-]+(::[0-9a-zA-Z_\\-]+){2}$");
	
	// sub createEnvelope{
	// my $other=shift;
	// my $user=shift;
	// $self->{LOGGER}->set_error_msg();
	// $self->info("$$ Ready to create the envelope for user $user (and @_)");
	//
	// $self->{UI}->execute("user","-", $user);
	//
	// $self->debug(1, "Executing access");
	// my $options=shift;
	// $options.= "v";
	// my (@info)=$self->{UI}->execute("access", $options, @_);
	// $self->info("$$ Everything is done for user $user (and @_)");
	// return @info;
	// }

	// --- for createEnvelope/access the signature will below look like
	// my $user=$self->{CONFIG}->{ROLE};
	// my $options = shift;
	// my $maybeoption = ( shift or 0 );
	// my $access;
	// if ( $maybeoption =~ /^-/ ) {
	// $options .= $maybeoption;
	// $access = (shift or 0),
	// } else {
	// $access = ( $maybeoption or 0);
	// }
	// my $lfns = (shift or 0);
	// my $se = (shift or "");
	// my $size = (shift or "0");
	// my $sesel = (shift or 0);
	// my $extguid = (shift or 0);
	// my $sitename= (shift || 0);
	// my $writeQos = (shift || 0);
	// my $writeQosCount = (shift || 0);
	


	/**
	 * 
	 * createEnvelope - the main entry function of Authen for the SOAP/Perl
	 * interoperability
	 * 
	 * @param user
	 * @param egal
	 * @param envreq
	 * @param lfn
	 * @param staticSEs
	 * @param size
	 * @param noSEs
	 * @param guid
	 * @param site
	 * @param qos
	 * @param qosCount
	 * @return
	 */
	public Map<String, String>[] createEnvelope(String P_user,
			String P_options, String P_maybeoptions, String P_lfn,
			String P_staticSEs, String P_size, String P_sesel_noSEs,
			String P_guid, String P_sitename, String P_qos, String P_qosCount) {

		P_user = ensureStringInitialized(P_user);
		if (P_user.length() == 0)
			return replySOAPerrorMessage_access_eof("No username provided");
		P_options = ensureStringInitialized(P_options);
		P_maybeoptions = ensureStringInitialized(P_maybeoptions);

		// if ( $maybeoption =~ /^-/ ) {
		// $options .= $maybeoption;
		// $access = (shift or 0),
		// } else {
		// $access = ( $maybeoption or 0);
		// }
		String P_access = "";
		String[] someoptions = P_maybeoptions.split(" ");
		for (String s : someoptions) {
			if (isPerlOption.matcher(P_access).matches()) {
				P_options += " " + s;
			} else {
				P_access = s;
			}
		}
		if (P_access.length() == 0)
			return replySOAPerrorMessage_access_eof("No access request provided");


		int access =  CatalogueAccess.INVALID;

		if (P_access != "read") { access = CatalogueAccess.READ; }
		else if (P_access != "delete") { access = CatalogueAccess.DELETE; }
		else if (writeRequest.matcher(P_access).matches()) { access = CatalogueAccess.WRITE;};
		if(access == CatalogueAccess.INVALID)
			return replySOAPerrorMessage_access_eof("Illegal access request provided, possible ones are: <read><write-version><write-once><delete>");
		

		P_lfn = ensureStringInitialized(P_lfn);
		if (P_lfn.length() == 0)
			return replySOAPerrorMessage_access_eof("No LFN provided");

		P_staticSEs = ensureStringInitialized(P_staticSEs);
		Set<String> ses = initializeSElist(P_staticSEs);

		P_size = ensureStringInitialized(P_size);
		int size = Integer.valueOf(P_size).intValue(); // here we still need to
														// ensure that ""
														// becomes 0

		P_sesel_noSEs = ensureStringInitialized(P_sesel_noSEs);
		int sesel = 0;
 
		if (isNumeric.matcher(P_sesel_noSEs).matches()) {
			sesel = Integer.valueOf(P_sesel_noSEs).intValue(); // here we still
																// need to
																// ensure that
																// "" becomes 0
			P_sesel_noSEs = "";
		}
		Set<String> exxSes = initializeSElist(P_sesel_noSEs);

		P_guid = ensureStringInitialized(P_guid);

		P_sitename = ensureStringInitialized(P_sitename);

		P_qos = ensureStringInitialized(P_qos);

		P_qosCount = ensureStringInitialized(P_qosCount);
		int qosCount = Integer.valueOf(P_qosCount).intValue(); // here we still
																// need to
																// ensure that
																// "" becomes 0

		AuthenServer authen = new AuthenServer();

		Set<XrootDEnvelope> envelopes = authen.createEnvelopePerlAliEnV218(P_user,
				access, P_options, P_lfn, size, P_guid, ses, exxSes,
				P_qos, qosCount, P_sitename);

		return translateEnvelopeIntoMap(envelopes);

	}

	public Map<String, String>[] translateEnvelopeIntoMap(
			Set<XrootDEnvelope> envelope) {

		// foreach envelope call and append String
		// envelope.getPerlEnvelopeTicket() to return;

		Map<String, String> returnMessage = new HashMap<String, String>();
		returnMessage.put("error", "AuthenX not implemented yet:");
		Map<String, String>[] returnAll = new HashMap[1];
		returnAll[0] = returnMessage;
		return returnAll;

	}

	/**
	 * 
	 * if the String s is null, set it to ""
	 * 
	 * @param s
	 * @return
	 */
	private String ensureStringInitialized(String s) {
		if (s == null) {
			return "";
		}
		return s;
	}

	/**
	 * 
	 * create a proper PerlAliEN-SOAP reply structure containing an error
	 * message
	 * 
	 * @param message
	 */
	private Map<String, String>[] replySOAPerrorMessage_access_eof(
			String message) {

		Map<String, String> returnMessage = new HashMap<String, String>();
		returnMessage
				.put("error", "AuthenX encountered an error in your request:"
						+ message + ".");
		Map<String, String>[] returnAll = new HashMap[1];
		returnAll[0] = returnMessage;
		return returnAll;
	}



	/**
	 * 
	 * initialize an array from the SE String "se1;se2;se3" containing only the
	 * valid SE names
	 * 
	 * @param sestring
	 * @return array of valid SE names as Strings
	 */
	private Set<String> initializeSElist(final String sestring) {

		final StringTokenizer st = new StringTokenizer(sestring, ";,");
		
		final Set<String> ret = new HashSet<String>();
		
		while (st.hasMoreTokens()){
			final String se = st.nextToken();
			
			final Matcher m = SE_NAME.matcher(se);
			
			if (m.matches())
				ret.add(se);
		}

		return ret;
	}


	//
	// sub doOperation {
	// my $other=shift;
	// my $user=shift;
	// my $directory=shift;
	// my $op=shift;
	// $self->info("$$ Ready to do an operation for $user in $directory (and $op '@_')");
	//
	// $self->{UI}->execute("user","-", $user);
	// my $mydebug=$self->{LOGGER}->getDebugLevel();
	// my $params=[];
	//
	// (my $tracelog,$params) =
	// AliEn::Util::findAndDropArrayElement("-tracelog", @_);
	// $tracelog and $self->{LOGGER}->tracelogOn();
	// (my $debug,$params) = AliEn::Util::getDebugLevelFromParameters(@$params);
	// $debug and $self->{LOGGER}->debugOn($debug);
	// # @_ = @{$params};
	// # $self->info("gron: params for call after cleaning are: @_");
	// $self->{LOGGER}->keepAllMessages();
	// $self->{UI}->{CATALOG}->{DISPPATH}=$directory;
	// my @info;
	// if ($op =~ /authorize/){
	// @info = $self->{UI}->{CATALOG}->authorize(@_);
	// } else {
	// @info = $self->{UI}->execute($op, @_);
	// }
	// my @loglist = @{$self->{LOGGER}->getMessages()};
	//
	// $debug and $self->{LOGGER}->debugOn($mydebug);
	// $self->{LOGGER}->tracelogOff();
	// $self->{LOGGER}->displayMessages();
	// $self->info("$$ doOperation DONE for user $user (and @_)");#, rc = $rc");
	// $self->info("$$ doOperation result: @info".scalar(@info));
	// return { #rc=>$rc,
	// rcvalues=>\@info, rcmessages=>\@loglist};
	// }

}
