package alien.tsealedEnvelope;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.Security;
import java.security.Signature;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Stack;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

/**
 * This class does the decoding/decryption of a given authorization token which
 * has to apply to the follwing format:
 * 
 * -----BEGIN SEALED CIPHER----- .. .. (Base64-encoded cipher) .. -----END
 * SEALED CIPHER----- -----BEGIN SEALED ENVELOPE----- .. .. (Base64-encoded
 * envelope) .. -----END SEALED ENVELOPE-----
 * 
 * The result is an authorization token object.
 * 
 * 
 * @author Martin Radicke
 * 
 */
public class EncryptedAuthzToken {
	// static Logger logger =
	// LoggerFactory.getLogger(EncryptedAuthzToken.class);

	

	


	// delimiters used to split the raw token into Cipher and Sealed Envelope
	private final static String CYPHER_START = "-----BEGIN SEALED CIPHER-----";
	private final static String CYPHER_END = "-----END SEALED CIPHER-----";
	private final static String ENVELOPE_START = "-----BEGIN SEALED ENVELOPE-----";
	private final static String ENVELOPE_END = "-----END SEALED ENVELOPE-----";

	// Blowfish initialisation vector
	private final static byte[] BLOWFISH_IV = "$KJh#(}q".getBytes();

	// raw cipher and Sealed Envelope
	private StringBuffer cipherEncryptedBase64;
	private StringBuffer envelopeEncryptedBase64;

	// decrypted blowfish key
	private byte[] symmetricKey;

	// created blowfish key
	private SecretKeySpec freshBlowfish;

	// extracted SHA1-signature to verify envelope data
	private byte[] signature;

	// the envelope data itself (token payload)
	private byte[] envelope;

	// local private key
	// private RSAPrivateKey privKey;

	// remote (e.g. from the file catalogue) public key
	// private RSAPublicKey pubKey;

	// the four key pairs necessary for encryption
	private RSAPrivateKey AuthenPrivKey;
	private RSAPublicKey AuthenPubKey;
	private RSAPrivateKey SEPrivKey;
	private RSAPublicKey SEPubKey;

	static {
		// the security provider used for decryption/verification
		Security.addProvider(new BouncyCastleProvider());
	}

	/**
	 * 
	 * Creates a new instance for encryption.
	 * @param AuthenPrivKey 
	 * @param SEPubKey 
	 * 
	 * @throws GeneralSecurityException
	 */
	public EncryptedAuthzToken(RSAPrivateKey AuthenPrivKey,
			RSAPublicKey SEPubKey) throws GeneralSecurityException {

		this.AuthenPrivKey = AuthenPrivKey;
		this.SEPubKey = SEPubKey;
	}

	/**
	 * Does the actual creation and encryption/encoding of a token. This method
	 * should not be called for more than one times.
	 * @param message 
	 * 
	 * @return the encrypted envelope or NULL if signature could not be verified
	 * @throws GeneralSecurityException
	 */
	public String encrypt(String message) throws GeneralSecurityException {

		Envelope env = new Envelope();
		envelope = (env.create_ALICE_SE_Envelope(message)).getBytes();

//		System.out.println("starting encryption of:" + (new String(envelope)));

		// encrypt UUID/CIPHER with the remote public key
		// get RSA-sealed cipher (aka session- or symmetric key(
		encryptSealedCipher();
//		System.out.println("starting cipherEncrypted64:"
//				+ new String(cipherEncryptedBase64));

		// create signature and envelope with symmetric key using Blowfish
		encryptSealedEnvelope();
//		System.out.println("starting envelopeEncrypted64:"
//				+ new String(envelopeEncryptedBase64));

		return getToken();

	}

	/**
	 * Encrypts the first component of the sealed token, which contains the
	 * session key (aka symmetric key).
	 * 
	 * @throws GeneralSecurityException
	 */
	private void encryptSealedCipher() throws GeneralSecurityException {

		// encrypt UUID/CIPHER with the remote public key
		Cipher cipher = Cipher.getInstance("RSA/NONE/PKCS1Padding", "BC");

		cipher.init(Cipher.WRAP_MODE, SEPubKey);

		// fUUID="";
		// int32_t i;
		// // for (i=0; i < 116; i++) {
		// for (i=0; i < 16; i++) {
		// char c = 1+(int32_t) (255.0*rand()/(RAND_MAX+1.0));
		// fUUID+= c;
		// }
		// Random randomGenerator = new Random();

		// byte[] symkey = new byte[16];
		// (new Random()).nextBytes(symkey);
		// // symmetricKey = new SecretKeySpec(symkey,"Blowfish");
		// SecretKeySpec symKeySpec = new SecretKeySpec(symmetricKey, 0,
		// (symmetricKey.length - 1),
		// "Blowfish");
		
//
		KeyGenerator keyGenerator = KeyGenerator.getInstance("Blowfish", "BC");
		keyGenerator.init(128);
		freshBlowfish = (SecretKeySpec) keyGenerator.generateKey();
//		freshBlowfish = new SecretKeySpec(
//				freshBlowfish.getEncoded(), "Blowfish");
		


		freshBlowfish = new SecretKeySpec(
				freshBlowfish.getEncoded(), 0, 16, "Blowfish");
		
		freshBlowfish = new SecretKeySpec(
		"po5439aasdfsdf8d7sdklksklaakskkd65asdf1dsfg23w6".getBytes(), 0, 16, "Blowfish");
		
		
//		int TSEALED_KEY_LENGTH = 17;
//		
//		Random randomGenerator = new Random();
//		String keySeed = "";
//		for(int a=0; a<TSEALED_KEY_LENGTH;a+=1){
//			char c =  (char) ((333*randomGenerator.nextInt())%256);
//			keySeed += c;
//		}
//		
//		freshBlowfish = new SecretKeySpec(
//				keySeed.getBytes(), 0, TSEALED_KEY_LENGTH, "Blowfish");
//		System.out.println("we have encoded freshBlowfish: "
//		+ freshBlowfish.getEncoded().length);
		
		
		
		
		
		
	 
//		symmetricKey = new byte[16];
//		symmetricKey = symKeySpec.getEncoded();
		
//		freshBlowfish = new Bit17BlowFishKey();
		
		// for ()
		// (255.0*rand()/(RAND_MAX+1.0));
		// (new Random()).nextBytes(symmetricKey);
		// symmetricKey = "po54398d765123w6".getBytes();

//		System.out.println("we had the encoded symmetricKey: "
//				+ new String(symmetricKey));
//		System.out.println("we had the encoded symmetricKey length: "
//				+ symmetricKey.length);
//		System.out.println("we had the encoded symKeySpec: "
//				+ symKeySpec.getEncoded());
//		System.out.println("we had the encoded symKeySpec length: "
//				+ symKeySpec.getEncoded().length);

		// freshBlowfish = new
		// SecretKeySpec(freshBlowfish.toString().getBytes(),1,16, "Blowfish");

		// SecretKeySpec symKeySpec = new
		// SecretKeySpec(symmetricKey.toString().getBytes(), 0,
		// ((symmetricKey.toString().getBytes()).length-1), "Blowfish");

		// this.symmetricKey = new byte[symKeySpec.getEncoded().length];
		// this.symmetricKey = symKeySpec.getEncoded();

		// symmetricKey = key.getEncoded();
		// SecretKey key = new SecretKeySpec(symkey,"Blowfish");
		// symmetricKey = key.toString().getBytes();
		// System.out.println();
		// System.out.println("creating the key ...");
		// System.out.println("we created the symkey: " +
		// freshBlowfish.toString());
		// System.out.println("symkey to String length:" +
		// freshBlowfish.toString().length());
		// System.out.println();
		// System.out.println("we created the encoded symkey: " +
		// freshBlowfish.getEncoded().toString());
		// System.out.println("encoded symkey length:" +
		// freshBlowfish.getEncoded().length);
		// System.out.println();
		// System.out.println("encoded base64 symkey:" +
		// Base64.encodeBytes(freshBlowfish.getEncoded()));
		//
		// System.out.println("cipher to String base64 cipher:"
		// + Base64.encodeBytes(freshBlowfish.toString().getBytes()));

		// System.out.println("choosen cipher is:"
		// + Base64.encodeBytes(key.toString().getBytes()));
		byte[] encryptedCipher = cipher.wrap(freshBlowfish);
//		 byte[] encryptedCipher = cipher.wrap(symKeySpec);

		// encode base64
		String sCipherEncryptedBase64 = Base64.encodeBytes(encryptedCipher);
		this.cipherEncryptedBase64 = new StringBuffer(
				sCipherEncryptedBase64.length());
		this.cipherEncryptedBase64.append(sCipherEncryptedBase64);

//		System.out.println("...key creation done!");
//		System.out.println();
	}

	/**
	 * Encrypts the actual envelope (the 2nd component) using the symmetric key
	 * and extracts the signature.
	 * 
	 * @throws GeneralSecurityException
	 */
	private void encryptSealedEnvelope() throws GeneralSecurityException {

		signature = signEnvelope();
//		System.out.println("we created the signature: "
//				+ Base64.encodeBytes(signature));

//		SecretKeySpec symKeySpec = new SecretKeySpec(symmetricKey, 0, 
//				(symmetricKey.length - 1), "Blowfish");
//		
//		SecretKeySpec symKeySpec = new SecretKeySpec(symmetricKey, 0, 16, "Blowfish");
		
		
		
		// SecretKeySpec symKeySpec = new SecretKeySpec(symmetricKey,
		// "Blowfish");
		// Cipher cipher =
		// Cipher.getInstance("Blowfish/CBC/PKCS5Padding","SunJCE");

		// System.out.println();
		// System.out.println("creating the key ...");
		// System.out.println("we created the symkey: " +
		// symKeySpec.toString());
		// System.out.println("symkey to String length:" +
		// symKeySpec.toString().length());
		// System.out.println();
//		System.out.println("we had the encoded symkey: "
//				+ new String(symmetricKey));
//		System.out.println("we had the encoded symkey length: "
//				+ symmetricKey.length);
//		System.out.println("we loaded the encoded symkey: "
//				+ new String(symKeySpec.getEncoded()));
//		System.out.println("we loaded the encoded symkey length: "
//				+ symKeySpec.getEncoded().length);

		// System.out.println("encoded symkey length:" +
		// symKeySpec.getEncoded().length);
		// System.out.println();
		// System.out.println("encoded base64 symkey:" +
		// Base64.encodeBytes(symKeySpec.getEncoded()));
		//
		// System.out.println("cipher to String base64 cipher:"
		// + Base64.encodeBytes(symKeySpec.toString().getBytes()));
		//

		Cipher cipher = Cipher.getInstance("Blowfish/CBC/PKCS5Padding", "BC");
		// Cipher cipher = Cipher.getInstance("Blowfish");
		// Encrypt!

		cipher.init(Cipher.ENCRYPT_MODE, freshBlowfish, new IvParameterSpec(
				BLOWFISH_IV));

//		cipher.init(Cipher.ENCRYPT_MODE, symKeySpec, new IvParameterSpec(
//				BLOWFISH_IV));
		//
		// byte[] encryptedEnvelope = cipher.doFinal(envelope, 0,
		// envelope.length-1);

		byte[] encryptedEnvelope = cipher.doFinal(envelope);

		// CipherParameters params = new ParametersWithIV(new
		// KeyParameter(symmetricKey,0,(symmetricKey.length-1)), BLOWFISH_IV);

		// CBCBlockCipher bc = new CBCBlockCipher(new BlowfishEngine());
		//
		// bc.init(true, new ParametersWithIV(new
		// KeyParameter(symmetricKey,0,(symmetricKey.length-1)), BLOWFISH_IV));
		//
		// byte[] encryptedEnvelope = new byte[envelope.length];
		//
		// int limit = envelope.length/bc.getBlockSize();
		//
		// if ((envelope.length%bc.getBlockSize()) > 0) {
		// limit += 1;
		// }
		// System.out.println("block size: "+ bc.getBlockSize() + " steps:" +
		// limit);
		// for (int i = 0;i<limit;i+=1) {
		// bc.processBlock(envelope, i, encryptedEnvelope, i);
		// System.out.println("baby step:  "+ i);
		// }
		//
		// // printArray("decrypted env", envelope, 0, envelope.length);
		// // logger.debug("decrypted cleartext:\n"+new String(unencrypted));
		//
		//
		//
		// System.out.println("OK, block cipher ran over the blob.");

		// Base64-decode envelope

		byte[] encryptedEnvelopeFinal = new byte[encryptedEnvelope.length + 4
				+ signature.length];

		// ByteBuffer buffer = ByteBuffer.allocate(4);
		// buffer.putInt(signature.length);
		// System.arraycopy(buffer.array(), 0, encryptedEnvelopeFinal, 0, 4);

		// encryptedEnvelopeFinal[0] =(byte) ((byte)( signature.length >> 24 ) &
		// 0xff);
		// encryptedEnvelopeFinal[1] =(byte) ((byte)( (signature.length << 8) >>
		// 24 ) & 0xff);
		// encryptedEnvelopeFinal[2] =(byte) ((byte)( (signature.length << 16)
		// >> 24 ) & 0xff);
		// encryptedEnvelopeFinal[3] =(byte) ((byte)( (signature.length << 24)
		// >> 24 ) & 0xff);

		encryptedEnvelopeFinal[0] = ((byte) (signature.length >> 24));
		encryptedEnvelopeFinal[1] = ((byte) ((signature.length << 8) >> 24));
		encryptedEnvelopeFinal[2] = ((byte) ((signature.length << 16) >> 24));
		encryptedEnvelopeFinal[3] = ((byte) ((signature.length << 24) >> 24));

//		System.out.println("siglen: " + signature.length + "");

		// //
		// int controlvalue = encryptedEnvelopeFinal[0] & 0xff << 24
		// | encryptedEnvelopeFinal[1] & 0xff << 16 | encryptedEnvelopeFinal[2]
		// & 0xff << 8 | encryptedEnvelopeFinal[3] & 0xff;
		//
		// System.out.println("our signature value is:"+ signature.length);
		//
		// System.out.println("our signature value is coded: "+ controlvalue);

		// encryptedEnvelopeFinal[0] = (byte) (encryptedEnvelopeFinal[0] &
		// 0xff);
		// encryptedEnvelopeFinal[1] = (byte) (encryptedEnvelopeFinal[1] &
		// 0xff);
		// encryptedEnvelopeFinal[2] = (byte) (encryptedEnvelopeFinal[2] &
		// 0xff);
		// encryptedEnvelopeFinal[3] = (byte) (encryptedEnvelopeFinal[3] &
		// 0xff);

		// System.arraycopy(signatureLength, 0, encryptedEnvelopeFinal, 0, 4);
		System.arraycopy(signature, 0, encryptedEnvelopeFinal, 4,
				signature.length);
		String hexsign = toHex(signature);
//		System.out.println("signature: --" + hexsign + "--");

		System.arraycopy(encryptedEnvelope, 0, encryptedEnvelopeFinal,
				4 + signature.length, encryptedEnvelope.length);

		String sEnvelopeEncryptedBase64 = Base64
				.encodeBytes(encryptedEnvelopeFinal);
		this.envelopeEncryptedBase64 = new StringBuffer(
				sEnvelopeEncryptedBase64.length());
		this.envelopeEncryptedBase64.append(sEnvelopeEncryptedBase64);

	}

	private static final char[] DIGITS = new char[] { '0', '1', '2', '3', '4', '5',
			'6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

	/**
	 * @param bytes
	 * @return hex string
	 */
	public String toHex(byte[] bytes) {
		char[] out = new char[bytes.length * 2]; // 2 hex characters per byte
		for (int i = 0; i < bytes.length; i++) {
			out[2 * i] = DIGITS[bytes[i] < 0 ? 8 + (bytes[i] + 128) / 16
					: bytes[i] / 16]; // append sign bit for negative bytes
			out[2 * i + 1] = DIGITS[bytes[i] < 0 ? (bytes[i] + 128) % 16
					: bytes[i] % 16];
		}
		return new String(out); // char sequence to string
	}

	/**
	 * Verifies the authenticity of the envelope by comparing the SHA1 hash of
	 * the envlope with the signature
	 * 
	 * @return true after successful verifi cation
	 * @throws GeneralSecurityException
	 */
	private byte[] signEnvelope() throws GeneralSecurityException {

		// // Compute digest
		// MessageDigest sha1 = MessageDigest.getInstance("SHA1");
		// byte[] digest = sha1.digest(envelope);
		// System.out.println("digest is:"+ new String(digest));
		// System.out.println("digest length:"+ digest.length);
		// Cipher cipher = Cipher.getInstance("RSA/NONE/PKCS1Padding", "BC");
		//
		// // Cipher cipher = Cipher.getInstance("RSA");
		// cipher.init(Cipher.ENCRYPT_MODE, AuthenPrivKey);
		// return cipher.doFinal(digest);

		//
		Signature signer = Signature.getInstance("SHA1withRSA", "BC");
		// // Signature signer = Signature.getInstance("RSA");
		signer.initSign(AuthenPrivKey);
		signer.update(envelope);
		// signer.update(digest);
		return signer.sign();
	}

	/**
	 * Splits the raw token (see class description for format) into its two
	 * components cipher and envelope
	 * 
	 * @param rawToken
	 *            the token which is going to be splitted
	 * @throws GeneralSecurityException
	 */
	private String getToken() throws GeneralSecurityException {

		return CYPHER_START + "\n" + cipherEncryptedBase64.toString() + "\n"
				+ CYPHER_END + "\n" + ENVELOPE_START + "\n"
				+ envelopeEncryptedBase64.toString() + "\n" + ENVELOPE_END
				+ "\n";
	}

	/**
	 * 
	 * Creates a new decryption instance.
	 * 
	 * @param rawToken
	 *            rawToken the sealed token which is going to be decrypted
	 * @param SEPrivKey 
	 * @param AuthenPubKey 
	 * @throws GeneralSecurityException
	 */
	public EncryptedAuthzToken(String rawToken, RSAPrivateKey SEPrivKey,
			RSAPublicKey AuthenPubKey) throws GeneralSecurityException {
		this.SEPrivKey = SEPrivKey;
		this.AuthenPubKey = AuthenPubKey;

		// split token into cipher and envelope
		splitToken(rawToken);
	}

	/**
	 * Does the actual decryption/decoding of the raw token. This method should
	 * not be called for more than one times.
	 * 
	 * @return the decrypted envelope or NULL if signature could not be verified
	 * @throws GeneralSecurityException
	 */
	public String decrypt() throws GeneralSecurityException {
		// get RSA-sealed cipher (aka session- or symmetric key(
		decryptSealedCipher();

		System.out.println("sealed cipher decrypted");
		// decrypt signature and envelope with symmetric key using Blowfish
		decryptSealedEnvelope();
		System.out.println("sealed envelope decrypted");
		// verify envelope using the signature
		if (!verifyEnvelope()) {
			System.out.println("VERIFICATION FAILED!");
			return null;
		}

		return new String(envelope);
	}

	/**
	 * Decrypts the first component of the sealed token, which contains the
	 * session key (aka symmetric key).
	 * 
	 * @throws GeneralSecurityException
	 */
	private void decryptSealedCipher() throws GeneralSecurityException {

		// decode base64
		byte[] encryptedCipher = Base64
				.decode(cipherEncryptedBase64.toString());

		// RSA-decrypt the session key by using the local private key
		Cipher cipher = Cipher.getInstance("RSA/NONE/PKCS1Padding", "BC");
//		System.out.println("do I like the key  .... ?");

		cipher.init(Cipher.UNWRAP_MODE, SEPrivKey);

		System.out.println("about to decode ....");

		// symmetricKey = (SecretKey) cipher.unwrap(encryptedCipher, "Blowfish",
		// Cipher.SECRET_KEY);

		Key key = cipher.unwrap(encryptedCipher, "Blowfish", Cipher.SECRET_KEY);

		symmetricKey = key.getEncoded();

		// symmetricKey = cipher.unwrap(encryptedCipher, "Blowfish",
		// Cipher.SECRET_KEY).getEncoded();

		// symmetricKey = new SecretKeySpec(key, 0, key.length,"Blowfish");
//		System.out.println("the encoded cipher length:" + symmetricKey.length);
//		System.out.println("encoded base64 cipher:"
//				+ Base64.encodeBytes(symmetricKey));

//		System.out
//				.println("cipher to String length:" + key.toString().length());
//		System.out.println("cipher to String base64 cipher:"
//				+ Base64.encodeBytes(key.toString().getBytes()));
	}

	/**
	 * Decrypts the actual envelope (the 2nd component) using the symmetric key
	 * and extracts the signature.
	 * 
	 * @throws GeneralSecurityException
	 */
	private void decryptSealedEnvelope() throws GeneralSecurityException {

		// Base64-decode envelope
		byte[] encryptedEnvelope = Base64.decode(envelopeEncryptedBase64
				.toString());
		// logger.debug("Sealed envelope total: "+encryptedEnvelope.length);

		// envelope format:
		// ================
		// 1. signature_length[4] !! integer in big endian (network byte order)
		// 2. signature[signature_length] !! RSA-privately encypted SHA1-hash of
		// envelope_data
		// 3. envelope_data[encryptedEnvelope.length - signature_length - 4] !!
		// the payload of the token, Blowfish-encrypted

		// usually big endian, but for legacy reasons little endian for now
		// (going to be changed in next
		// Alien file catalogue version

		// big endian
		int signatureLength = encryptedEnvelope[0] & 0xff << 24
				| encryptedEnvelope[1] & 0xff << 16 | encryptedEnvelope[2]
				& 0xff << 8 | encryptedEnvelope[3] & 0xff;
		// int signatureLength = encryptedEnvelope[0] & 0xff |
		// encryptedEnvelope[1] & 0xff << 8 | encryptedEnvelope[2] & 0xff << 16
		// | encryptedEnvelope[3] & 0xff << 24;
		int envelopeOffset = 4 + signatureLength;
//		System.out.println("Found signature length:" + signatureLength);
		// store signature into a seperate buffer
		signature = new byte[signatureLength];
		System.arraycopy(encryptedEnvelope, 4, signature, 0, signatureLength);

		// String hexsign = toHex(signature);

//		System.out.println("signature: --" + Base64.encodeBytes(signature)
//				+ "--");
//		System.out.println("siglen: " + signature.length + "");

		// if (logger.getEffectiveLevel().equals(Level.DEBUG)) {
		// printArrayHex("encrypted env", encryptedEnvelope, envelopeOffset,
		// encryptedEnvelope.length-envelopeOffset);
		// logger.debug("siglength="+signatureLength+"  envelopeOffset="+envelopeOffset);
		// }

		// stripe off trailing zero (the key is stored as a zero-padded array
		// for easier string handling in C)
		// int keylen = symmetricKey.toString().getBytes().length - 1;

		// printArrayHex("symmetric key (length="+keylen*8+" bit)",
		// symmetricKey, 0, keylen);
		// printArrayHex("iv", BLOWFISH_IV, 0, BLOWFISH_IV.length);

		// ////////////////////////////////////////////
		// BC Blowfish, native interface
		// ////////////////////////////////////////////

		// CipherParameters params = new ParametersWithIV(new
		// KeyParameter(symmetricKey,0,keylen), iv);
		// CBCBlockCipher bc = new CBCBlockCipher(new BlowfishEngine());
		// bc.init(false, params);
		//
		// byte[] unencrypted = new
		// byte[encryptedEnvelope.length-envelopeOffset];
		//
		// for (int i = 0;i<encryptedEnvelope.length-envelopeOffset;i=i+8) {
		// bc.processBlock(encryptedEnvelope, envelopeOffset+i, unencrypted, i);
		// }
		//
		// printArray("decrypted env", unencrypted, 0, unencrypted.length);
		// logger.debug("decrypted cleartext:\n"+new String(unencrypted));

		// ////////////////////////////////////////////
		// SunJCE/BC Blowfish
		// ////////////////////////////////////////////
		//
//		SecretKeySpec symKeySpec = new SecretKeySpec(symmetricKey, 0,
//				(symmetricKey.length - 1), "Blowfish");
		SecretKeySpec symKeySpec = new SecretKeySpec(symmetricKey, 0,
				(symmetricKey.length), "Blowfish");
		// SecretKeySpec symKeySpec = new
		// SecretKeySpec(symmetricKey,"Blowfish");

		// SunJCE Provider doing blowfish decrypt (how about performance?)
		// Cipher cipher =
		// Cipher.getInstance("Blowfish/CBC/PKCS5Padding","SunJCE");

		// BC provider doing blowfish decryption
		Cipher cipher = Cipher.getInstance("Blowfish/CBC/PKCS5Padding", "BC");
		cipher.init(Cipher.DECRYPT_MODE, symKeySpec, new IvParameterSpec(
				BLOWFISH_IV));
		envelope = cipher.doFinal(encryptedEnvelope, envelopeOffset,
				encryptedEnvelope.length - envelopeOffset);

		// if (logger.getEffectiveLevel().equals(Level.DEBUG)) {
		// logger.debug("encrypted envelope length="+(encryptedEnvelope.length -
		// envelopeOffset)+"   unencrypted envelope length="+ envelope.length);
		// printArrayHex("decrypted env", envelope, 0, envelope.length);
		// logger.debug("decrypted cleartext:\n"+new String(envelope));
		// }

		// ////////////////////////////////////////////
		// BlowfishJ (does not work because the lack of Padding)
		// ////////////////////////////////////////////

		// BlowfishCBC bfc = new BlowfishCBC(key3,0,key3.length, new
		// BigInteger(iv).longValue());
		//
		// if (bfc.weakKeyCheck())
		// {
		// logger.debug("CBC key is weak!");
		// }
		// else
		// {
		// logger.debug("CBC key OK");
		// }
		//
		// logger.debug(encryptedEnvelope.length+" "+envelopeOffset+" "+(encryptedEnvelope.length-envelopeOffset));
		// bfc.decrypt(encryptedEnvelope, envelopeOffset, unencrypted, 0,
		// encryptedEnvelope.length - envelopeOffset);
		//
		// printArray("decrypted env", unencrypted, 0, unencrypted.length);
		// logger.debug("decrypted cleartext:\n"+new String(unencrypted));
	}

	/**
	 * Verifies the authenticity of the envelope by comparing the SHA1 hash of
	 * the envlope with the signature
	 * 
	 * @return true after successful verification
	 * @throws GeneralSecurityException
	 */
	private boolean verifyEnvelope() throws GeneralSecurityException {

		// // Compute digest
		// MessageDigest sha1 = MessageDigest.getInstance("SHA1");
		// byte[] digest = sha1.digest(envelope);
		// System.out.println("digest is:"+ new String(digest));
		//
		// System.out.println("digest length:"+ digest.length);
		// Cipher cipher = Cipher.getInstance("RSA/NONE/PKCS1Padding", "BC");
		//
		// // Cipher cipher = Cipher.getInstance("RSA");
		// cipher.init(Cipher.DECRYPT_MODE, AuthenPrivKey);
		// return (cipher.doFinal(signature) == digest);

		// Signature signer = Signature.getInstance("SHA1withRSA");
		// // Signature signer = Signature.getInstance("RSA");
		// signer.initVerify(AuthenPubKey);
		// // signer.update(envelope);
		// signer.update(digest);
		// return signer.verify(signature);

		Signature signer = Signature.getInstance("SHA1withRSA", "BC");
		signer.initVerify(AuthenPubKey);
		signer.update(envelope);
		return signer.verify(signature);
	}

	/**
	 * Splits the raw token (see class description for format) into its two
	 * components cipher and envelope
	 * 
	 * @param rawToken
	 *            the token which is going to be splitted
	 * @throws GeneralSecurityException
	 */
	private void splitToken(String rawToken) throws GeneralSecurityException {
		cipherEncryptedBase64 = new StringBuffer();
		envelopeEncryptedBase64 = new StringBuffer();

		Stack<String> stack = new Stack<String>();

		LineNumberReader input = new LineNumberReader(
				new StringReader(rawToken));

		try {
			String line = null;

			while ((line = input.readLine()) != null) {

				if (line.equals(CYPHER_START)) {
					stack.push(CYPHER_START);
					continue;
				}

				if (line.equals(CYPHER_END)) {
					if (!stack.peek().equals(CYPHER_START)) {
						throw new GeneralSecurityException(
								"Illegal format: Cannot parse encrypted cipher");
					}
					stack.pop();
					continue;
				}

				if (line.equals(ENVELOPE_START)) {
					// check if ENVELOPE part is not nested in CYPHER part
					if (!stack.isEmpty()) {
						throw new GeneralSecurityException(
								"Illegal format: Cannot parse encrypted envelope");
					}
					stack.push(ENVELOPE_START);
					continue;
				}

				if (line.equals(ENVELOPE_END)) {
					if (!stack.peek().equals(ENVELOPE_START)) {
						throw new GeneralSecurityException(
								"Illegal format: Cannot parse encrypted envelope");
					}
					stack.pop();
					continue;
				}

				if (stack.isEmpty()) {
					continue;
				}

				if (stack.peek().equals(CYPHER_START)) {
					cipherEncryptedBase64.append(line);
					continue;
				}

				if (stack.peek().equals(ENVELOPE_START)) {
					envelopeEncryptedBase64.append(line);
					continue;
				}
			}

		} catch (IOException e) {
			throw new GeneralSecurityException(
					"error reading from token string");
		}

		try {
			input.close();
		} catch (IOException e) {
			throw new GeneralSecurityException(
					"error closing stream where token string was parsed from");
		}

	}

	/**
	 * Helper method to print out anarray in hex notation.
	 * 
	 * @param name
	 *            the name to prefix the hex dump
	 * @param array
	 *            the array which will be dumped
	 * @param offset
	 *            the offset from where the dump will start
	 * @param len
	 *            the number of bytes to be dumped
	 */
	@SuppressWarnings("unused")
	private String arrayToHex(String name, byte[] array, int offset, int len) {
		if (array == null) {
			return "";
		}

		StringBuffer sb = new StringBuffer(name + ": ");
		for (int i = offset; i < offset + len; i++) {
			String s = Integer.toHexString(array[i] & 0xff);
			if (s.length() == 1) {
				sb.append("0");
			}
			sb.append(s.toUpperCase());

		}

		sb.append(" (total:");
		sb.append(len);
		sb.append(" bytes)");

		return sb.toString();
	}

	/**
	 * This method parses the decrypted envelope and returns its representation
	 * object.
	 * 
	 * @return an envelope object
	 * @throws GeneralSecurityException
	 *             is thrown if envelope has expired
	 * @throws CorruptedEnvelopeException
	 *             is thrown if a parsing error occurs
	 */
	public Envelope getEnvelope() throws CorruptedEnvelopeException,
			GeneralSecurityException {
		return new Envelope(new String(envelope));
	}

	/**
	 * @return the envelope
	 */
	public String getEnvelopeString() {
		return new String(envelope);
	}

}
