import java.io.*;
import java.security.*;
import java.security.spec.*;
 
public class VSNKeyGenerator {
 
	public static void main(String args[]) {
		VSNKeyGenerator vsnkeygen= new VSNKeyGenerator();
		try {
			String path = "";
 
			KeyPairGenerator keyGen = KeyPairGenerator.getInstance("DSA");
            SecureRandom rng = SecureRandom.getInstance("SHA1PRNG", "SUN");
			rng.setSeed(1024);
			keyGen.initialize(1024, rng);
			KeyPair keyPair = keyGen.genKeyPair();
 
			System.out.println("Generated Key Pair");
			
			PrivateKey privateKey = keyPair.getPrivate();
			PublicKey publicKey = keyPair.getPublic();
 
			// Store Public Key.
			X509EncodedKeySpec x509EncodedKeySpec = new X509EncodedKeySpec(
				publicKey.getEncoded());
			FileOutputStream out = new FileOutputStream(path + "public.key");
			out.write(x509EncodedKeySpec.getEncoded());
			out.close();
 
			// Store Private Key.
			PKCS8EncodedKeySpec pkcs8EncodedKeySpec = new PKCS8EncodedKeySpec(
				privateKey.getEncoded());
			out = new FileOutputStream(path + "private.key");
			out.write(pkcs8EncodedKeySpec.getEncoded());
			out.close();
				
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}
}