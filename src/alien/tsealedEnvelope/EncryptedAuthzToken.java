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
 * This class does the decoding/decryption + encoding/encryption of a given
 * authorization token which has to apply to the follwing format:
 * 
 * -----BEGIN SEALED CIPHER----- .. .. (Base64-encoded cipher) .. -----END
 * SEALED CIPHER----- -----BEGIN SEALED ENVELOPE----- .. .. (Base64-encoded
 * envelope) .. -----END SEALED ENVELOPE-----
 * 
 * The result is an authorization token object. Based on original dCache code
 * from Martin Radicke
 * 
 * @author ron
 * 
 */
public class EncryptedAuthzToken {

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
	 * Creates a new instance either for encryption or decryption
	 * 
	 * @param PrivKey
	 * @param PubKey
	 * @param Decrypt
	 * 
	 * @throws GeneralSecurityException
	 */
	public EncryptedAuthzToken(RSAPrivateKey PrivKey, RSAPublicKey PubKey,
			boolean Decrypt) throws GeneralSecurityException {

		if (Decrypt) {
			this.SEPrivKey = PrivKey;
			this.AuthenPubKey = PubKey;
		}

		this.AuthenPrivKey = PrivKey;
		this.SEPubKey = PubKey;
	}

	/**
	 * Does the actual creation and encryption/encoding of a token.
	 * 
	 * @param message
	 * 
	 * @return the encrypted envelope or NULL if signature could not be verified
	 * @throws GeneralSecurityException
	 */
	public String encrypt(String message) throws GeneralSecurityException {

		Envelope env = new Envelope();
		envelope = (env.create_ALICE_SE_Envelope(message)).getBytes();

		// System.out.println("starting encryption of:" + (new
		// String(envelope)));

		// encrypt UUID/CIPHER with the remote public key
		// get RSA-sealed cipher (aka session- or symmetric key(
		encryptSealedCipher();
		// System.out.println("starting cipherEncrypted64:"
		// + new String(cipherEncryptedBase64));

		// create signature and envelope with symmetric key using Blowfish
		encryptSealedEnvelope();
		// System.out.println("starting envelopeEncrypted64:"
		// + new String(envelopeEncryptedBase64));

		return getToken();

	}

	/**
	 * Encrypts the first component of the sealed token, which contains the
	 * session key (aka symmetric key).
	 * 
	 * @throws GeneralSecurityException
	 */
	private void encryptSealedCipher() throws GeneralSecurityException {

		Cipher cipher = Cipher.getInstance("RSA/NONE/PKCS1Padding", "BC");

		cipher.init(Cipher.WRAP_MODE, SEPubKey);
		KeyGenerator keyGenerator = KeyGenerator.getInstance("Blowfish", "BC");
		keyGenerator.init(128);
		freshBlowfish = (SecretKeySpec) keyGenerator.generateKey();

		final byte[] bkey = freshBlowfish.getEncoded();
		freshBlowfish = new SecretKeySpec(bkey, 0, 16, "Blowfish");

		final byte[] key = new byte[17];
		System.arraycopy(bkey, 0, key, 0, 16);
		key[16] = (byte) '\0';
		SecretKeySpec freshBlowfishDASHED = new SecretKeySpec(key, 0, 17,
				"Blowfish");

		byte[] encryptedCipher = cipher.wrap(freshBlowfishDASHED);

		// encode base64
		String sCipherEncryptedBase64 = Base64.encodeBytes(encryptedCipher);
		this.cipherEncryptedBase64 = new StringBuffer(
				sCipherEncryptedBase64.length());
		this.cipherEncryptedBase64.append(sCipherEncryptedBase64);

	}

	/**
	 * Encrypts the actual envelope (the 2nd component) using the symmetric key
	 * and extracts the signature.
	 * 
	 * @throws GeneralSecurityException
	 */
	private void encryptSealedEnvelope() throws GeneralSecurityException {

		signature = signEnvelope();

		Cipher cipher = Cipher.getInstance("Blowfish/CBC/PKCS5Padding", "BC");

		cipher.init(Cipher.ENCRYPT_MODE, freshBlowfish, new IvParameterSpec(
				BLOWFISH_IV));

		byte[] encryptedEnvelope = cipher.doFinal(envelope);

		// Base64-decode envelope
		byte[] encryptedEnvelopeFinal = new byte[encryptedEnvelope.length + 4
				+ signature.length];

		encryptedEnvelopeFinal[0] = ((byte) (signature.length >> 24));
		encryptedEnvelopeFinal[1] = ((byte) ((signature.length << 8) >> 24));
		encryptedEnvelopeFinal[2] = ((byte) ((signature.length << 16) >> 24));
		encryptedEnvelopeFinal[3] = ((byte) ((signature.length << 24) >> 24));

		System.arraycopy(signature, 0, encryptedEnvelopeFinal, 4,
				signature.length);

		System.arraycopy(encryptedEnvelope, 0, encryptedEnvelopeFinal,
				4 + signature.length, encryptedEnvelope.length);

		String sEnvelopeEncryptedBase64 = Base64
				.encodeBytes(encryptedEnvelopeFinal);
		this.envelopeEncryptedBase64 = new StringBuffer(
				sEnvelopeEncryptedBase64.length());
		this.envelopeEncryptedBase64.append(sEnvelopeEncryptedBase64);

	}

	private static final char[] DIGITS = new char[] { '0', '1', '2', '3', '4',
			'5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

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

		Signature signer = Signature.getInstance("SHA1withRSA", "BC");
		signer.initSign(AuthenPrivKey);
		signer.update(envelope);
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
	 * Does the actual decryption/decoding of the raw token. This method should
	 * not be called for more than one times.
	 * 
	 * @return the decrypted envelope or NULL if signature could not be verified
	 * @throws GeneralSecurityException
	 */
	public String decrypt(String rawToken) throws GeneralSecurityException {

		// split token into cipher and envelope
		splitToken(rawToken);

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
		cipher.init(Cipher.UNWRAP_MODE, SEPrivKey);

		Key key = cipher.unwrap(encryptedCipher, "Blowfish", Cipher.SECRET_KEY);

		symmetricKey = key.getEncoded();
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

		int envelopeOffset = 4 + signatureLength;

		// store signature into a seperate buffer
		signature = new byte[signatureLength];
		System.arraycopy(encryptedEnvelope, 4, signature, 0, signatureLength);

		SecretKeySpec symKeySpec = new SecretKeySpec(symmetricKey, 0,
				(symmetricKey.length - 1), "Blowfish");

		// BC provider doing blowfish decryption
		Cipher cipher = Cipher.getInstance("Blowfish/CBC/PKCS5Padding", "BC");
		cipher.init(Cipher.DECRYPT_MODE, symKeySpec, new IvParameterSpec(
				BLOWFISH_IV));
		envelope = cipher.doFinal(encryptedEnvelope, envelopeOffset,
				encryptedEnvelope.length - envelopeOffset);
	}

	/**
	 * Verifies the authenticity of the envelope by comparing the SHA1 hash of
	 * the envlope with the signature
	 * 
	 * @return true after successful verification
	 * @throws GeneralSecurityException
	 */
	private boolean verifyEnvelope() throws GeneralSecurityException {

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
