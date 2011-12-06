package alien;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashSet;

import alien.catalogue.BookingTable;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.io.protocols.Factory;
import alien.io.protocols.Protocol;
import alien.io.xrootd.envelopes.XrootDEnvelopeSigner;
import alien.se.SE;
import alien.se.SEUtils;
import alien.user.AliEnPrincipal;
import alien.user.UserFactory;


/**
 * Testing stuff
 * 
 * @author costing
 *
 */
public class Testing {
	
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws GeneralSecurityException 
	 */
	public static void main(String[] args) throws IOException, GeneralSecurityException {
		if (true){
			String s="-----BEGIN SEALED CIPHER-----\n" + 
					"oAgJ3jOO0lFT-vooC7Zqq8p-mFS8UMVYs3elz1ByUJEgl-VU7693dXhInhRGu7NEQqtv9GDUBmv+\n" + 
					"VXYpggh2wa-n+RfEhdxPCsBubU9qRO-R+pwX0NTJt51rxPWBoJIVKoEp9v9NcKV8NepToPZJv5Up\n" + 
					"PvM7YHe5bRRFTLcsml4=\n" + 
					"-----END SEALED CIPHER-----\n" + 
					"-----BEGIN SEALED ENVELOPE-----\n" + 
					"AAAAgFv84DIm6FUNQM+qV8SxelF7v2VnBG7v0G1QMIST2Ti9QYyjxpLtLwselUqbvn15bKBepnkr\n" + 
					"OSQ6ZDpCIUH4X-kFuOgcLkNKIvRtfiU+KauJluB-2HzbRhfD9MUBl7BC9D7s9TGjX8SMPqSICTrK\n" + 
					"dCe0uw8zcxUHSaP0owTVSeSOUOlzlOmkCZ--YFOzzcqlEcQI+X7St3LKufmsfJeiKr8pCcST0GyA\n" + 
					"R4dKjcIVoICrXO+G0EOFyVW9e0Cvugm9I9OTZGZ7K+ofz6IhPcWRvaOHQdgCzDH5S2yHS3fY16qg\n" + 
					"YNtVbkrIecRK2jWSOFeBBSfu5IXyyS9rMybdrMB6eQ1Ol0WePHqass0DBIdlZN-A6rYKNrtXGwwC\n" + 
					"-zJ9W2E8cMvRzMKJwd1GdAtGD8qlEeBwXSjVUJqU6dgZBcHqVNuNI+J2dpvSG-LJOUNvVsbkGOhU\n" + 
					"0l2AfE03iW71uiQ5pI6IvSljORNDFbcqfJnbhif82HIsMv+2DZyNuYOaynGrv4o4CEvqc9w2aZJb\n" + 
					"zzxC6Rv00reY9IJB4KLTbnfGIQMaxxxBxbhp0FcqAhXEdwSJoFoqpX8yUXBnkFQwRANz+k4ZDrk9\n" + 
					"Is5HBrTvruhqRfnWXCLGQXZGIpP3MGqOyCTM0ynyy-UdZna4zmG1+BHAZvUyYu8C+VVIkESb7T-4\n" + 
					"e1pjHWhwUtLK1NPnpNr8JeV+QjXVqwLzZCio8qq3F1G1aNNlezNbGCMYxA-lic0+-jBkIUoLDgOU\n" + 
					"udisiZ49LjYs7ItR2H6Ipzx4GDioIaZ7B-kmzSugukG1aQ4KX1LFt9uDqAFqIPzHrklTPkvV9z2U\n" + 
					"oaN+vee8M6ha8pRRPu3gLjWFitIUIUgSE3qNeWPxqEwbuOvWuJZ27J4xqehXvG7ESzyt5i-H3geF\n" + 
					"7biy1Zbst4yzG5n8zTCCGZ5k0EY7aC3gtETQNu7Mfv+SM2fLPv6mp2fkcbDmtWx-za2H1qg-y2uu\n" + 
					"9b830I1mbw==\n" + 
					"-----END SEALED ENVELOPE-----";
			
			System.err.println(XrootDEnvelopeSigner.decrypt(s));
			
			return;
		}
		
		AliEnPrincipal owner = UserFactory.getByUsername("aliprod");
		
		LFN targetLFN = LFNUtils.getLFN("/alice/cern.ch/user/a/aliprod/test/setest/ALICE::RAL::TAPE_test", true);
		
		SE se = SEUtils.getSE("ALICE::RAL::TAPE");
		
		File localFile = new File("/home/costing/pcalimonitor/bin/setesting/testfile");
		
		GUID guid = GUIDUtils.createGuid(localFile, owner);
		
		guid.lfnCache = new HashSet<LFN>();
		guid.lfnCache.add(targetLFN);
		
		PFN p = BookingTable.bookForWriting(targetLFN, guid, null, 0, se); 
		
		System.err.println(p);
		System.err.println(p.ticket.envelope.getEncryptedEnvelope());
		
		Protocol protocol = Factory.xrootd;
		
		String outcome = protocol.put(p, localFile);
		
		System.err.println(outcome);
	}
	
}
