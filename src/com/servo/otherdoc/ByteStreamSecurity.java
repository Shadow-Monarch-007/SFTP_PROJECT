package com.servo.otherdoc;

import com.servo.util.security.SecurityUtil;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import org.apache.commons.codec.binary.Base64;

public class ByteStreamSecurity {

    public static final int byteArrayLength = 50;
    Cipher cipherInstance;
    SecretKey secretKeyInstance;
    SecurityUtil secUtil = new SecurityUtil();

    public short byteArrayToShort(byte[] arr) {

        ByteBuffer wrapped = ByteBuffer.wrap(arr); // big-endian by default
        short num = wrapped.getShort(); // 1
        return num;
    }

    public byte[] intToByteArray(short num) {
        ByteBuffer dbuf = ByteBuffer.allocate(2);
        dbuf.putShort(num);
        byte[] bytes = dbuf.array(); // { 0, 1 }
        return bytes;
    }

    public byte[] byteInputStreamEncryption(byte[] inputFileBuffer) {

        int fileLength = inputFileBuffer.length;

        byte[] newInputByteArray = Arrays.copyOfRange(inputFileBuffer, 0, byteArrayLength);

        byte[] outputFileBuffer = null;

        try {

            cipherInstance = Cipher.getInstance("RC4");
            secretKeyInstance = secUtil.getSecretKey("U0VSVk8=");

            byte[] encryptedByteArray = encrypt(newInputByteArray, cipherInstance, secretKeyInstance);

            int encryptedByteLength = encryptedByteArray.length;

            byte[] shortBytes = intToByteArray((short) encryptedByteLength);

            outputFileBuffer = new byte[fileLength - byteArrayLength + shortBytes.length + (encryptedByteLength)];

            System.arraycopy(shortBytes, 0, outputFileBuffer, 0, shortBytes.length);

            System.arraycopy(encryptedByteArray, 0, outputFileBuffer, shortBytes.length, encryptedByteArray.length);

            System.arraycopy(inputFileBuffer, byteArrayLength, outputFileBuffer, encryptedByteArray.length + shortBytes.length, inputFileBuffer.length - byteArrayLength);

        } catch (NoSuchPaddingException ex) {
            Logger.getLogger(ByteStreamSecurity.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NoSuchAlgorithmException ex) {
            Logger.getLogger(ByteStreamSecurity.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(ByteStreamSecurity.class.getName()).log(Level.SEVERE, null, ex);
        }

        return outputFileBuffer;

    }

    public byte[] byteInputStreamDecryption(byte[] inputFileBuffer) {

        byte[] outputFileBuffer = null;

        try {

            short shortBytesLength = 2;
            cipherInstance = Cipher.getInstance("RC4");
            secretKeyInstance = secUtil.getSecretKey("U0VSVk8=");

            short lengthOfEncryptedByteArray = byteArrayToShort(Arrays.copyOfRange(inputFileBuffer, 0, shortBytesLength));

            System.out.println(" a.byteArrayToShort(Arrays.copyOfRange(outputFileBuffer, 0, 2)) : " + lengthOfEncryptedByteArray);

            byte[] decryptedByteArray = decrypt(Arrays.copyOfRange(inputFileBuffer, shortBytesLength, lengthOfEncryptedByteArray + shortBytesLength), cipherInstance, secretKeyInstance);

            System.out.println("decryptedByteArray : " + new String(decryptedByteArray));
            System.out.println("............... " + decryptedByteArray.length);

            outputFileBuffer = new byte[decryptedByteArray.length + (inputFileBuffer.length - lengthOfEncryptedByteArray - shortBytesLength)];

            System.arraycopy(decryptedByteArray, 0, outputFileBuffer, 0, decryptedByteArray.length);

            System.arraycopy(inputFileBuffer, lengthOfEncryptedByteArray + shortBytesLength, outputFileBuffer, decryptedByteArray.length, inputFileBuffer.length - (lengthOfEncryptedByteArray + shortBytesLength));
            //System.out.println("final decrypted output is : " + new String(outputFileBuffer));

        } catch (Exception ex) {
            Logger.getLogger(ByteStreamSecurity.class.getName()).log(Level.SEVERE, null, ex);
        }

        return outputFileBuffer;

    }

    public byte[] encrypt(byte[] plainTextByteArray, Cipher cipher, SecretKey secretKey) {

        byte[] encryptedByte = null;
        try {
            cipher.init(1, secretKey);
            byte[] byteCipherText = cipher.doFinal(plainTextByteArray);
//            encryptedText = new Base64().encodeToString(byteCipherText);  // appends newline character (Sanjay Shukla)
            encryptedByte = Base64.encodeBase64(byteCipherText);

            //byte[] encryptedPasswordByte = encryptedText.getBytes();
//            System.out.println("QQQQQQQ - > " + encryptedPasswordByte);
//            if (encryptedPasswordByte[encryptedPasswordByte.length - 4] == 0x00 && encryptedPasswordByte[encryptedPasswordByte.length - 3] == 0x0D
//                    && encryptedPasswordByte[encryptedPasswordByte.length - 2] == 0x00
//                    && encryptedPasswordByte[encryptedPasswordByte.length - 1] == 0x0A) {
//                encryptedPasswordByte = Arrays.copyOfRange(encryptedPasswordByte, 0, encryptedPasswordByte.length - 4);
//                encryptedText = encryptedPasswordByte.toString();
//            }
//            System.out.print(""+encryptedText.getBytes(encryptedText));
//            System.out.println("");
//
//            for (int i = 0; i < encryptedPasswordByte.length; i++) {
//                System.out.print("[encryptedPasswordByte] : [" + i + "] --- > " + (encryptedPasswordByte[i] & 0xFF));
//            }
//            if (encryptedByte[encryptedByte.length - 1] == '\n' && encryptedByte[encryptedByte.length - 2] == '\r') {
//          
////              encryptedText = encryptedText.substring(0, encryptedText.length() - 2);
//                encryptedByte = Arrays.copyOfRange(0,encryptedByte.length - 2);
//            }
//               if (encryptedText.charAt(encryptedText.length() - 1) == '\n' && encryptedText.charAt(encryptedText.length() - 2) == '\r') {
//                encryptedText = encryptedText.substring(0, encryptedText.length() - 2);
//            }
        } catch (InvalidKeyException invalidKey) {
            System.out.println(" Invalid Key " + invalidKey);
        } catch (IllegalBlockSizeException ex) {
            Logger.getLogger(ByteStreamSecurity.class.getName()).log(Level.SEVERE, null, ex);
        } catch (BadPaddingException ex) {
            Logger.getLogger(ByteStreamSecurity.class.getName()).log(Level.SEVERE, null, ex);
        }
        return encryptedByte;

    }
    
    public byte[] decrypt(byte[] plainTextByteArray, Cipher cipher, SecretKey secretKey) throws Exception {

        byte[] byteDecryptedText = null;


        try {
            cipher.init(2, secretKey);
//            byte[] byteDecryptedText = cipher.doFinal(new Base64().decode(encryptedText));//  SANJAY SHUKLA
            byteDecryptedText = cipher.doFinal(Base64.decodeBase64(plainTextByteArray));
//            plainText = new String(byteDecryptedText);
        } catch (InvalidKeyException ex) {
            throw new Exception(ex.getMessage());
        } catch (BadPaddingException ex) {
            throw new Exception(ex.getMessage());
        } catch (IllegalBlockSizeException ex) {
            throw new Exception(ex.getMessage());
        } catch (Exception ex) {
            throw new Exception(ex.getMessage());
        }
        return byteDecryptedText;

//    return null;
    }
}
