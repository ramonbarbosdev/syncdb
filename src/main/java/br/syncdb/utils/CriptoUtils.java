package br.syncdb.utils;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Base64;

public class CriptoUtils {

    private static final int GCM_IV_LENGTH = 12; // 96 bits
    private static final int GCM_TAG_LENGTH = 128; // bits

    public static String criptografar(String plaintext, byte[] chave) throws Exception {
        byte[] iv = new byte[GCM_IV_LENGTH];
        new SecureRandom().nextBytes(iv);

        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        SecretKey secretKey = new SecretKeySpec(chave, "AES");
        GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
        cipher.init(Cipher.ENCRYPT_MODE, secretKey, spec);

        byte[] ciphertext = cipher.doFinal(plaintext.getBytes("UTF-8"));

        byte[] finalData = new byte[iv.length + ciphertext.length];
        System.arraycopy(iv, 0, finalData, 0, iv.length);
        System.arraycopy(ciphertext, 0, finalData, iv.length, ciphertext.length);

        return Base64.getEncoder().encodeToString(finalData);
    }

    public static String descriptografar(String base64, byte[] chave) throws Exception {
        byte[] data = Base64.getDecoder().decode(base64);

        byte[] iv = new byte[GCM_IV_LENGTH];
        byte[] ciphertext = new byte[data.length - GCM_IV_LENGTH];

        System.arraycopy(data, 0, iv, 0, GCM_IV_LENGTH);
        System.arraycopy(data, GCM_IV_LENGTH, ciphertext, 0, ciphertext.length);

        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        SecretKey secretKey = new SecretKeySpec(chave, "AES");
        GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
        cipher.init(Cipher.DECRYPT_MODE, secretKey, spec);

        byte[] plain = cipher.doFinal(ciphertext);
        return new String(plain, "UTF-8");
    }

    public static byte[] gerarChave256(String segredo) {
        byte[] key = new byte[32]; // 256 bits
        byte[] segredoBytes = segredo.getBytes();
        System.arraycopy(segredoBytes, 0, key, 0, Math.min(segredoBytes.length, key.length));
        return key;
    }
}