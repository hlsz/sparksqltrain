package com.data.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Util {

    public static String encryptMD5(final String srcStr){
        return encrypt(srcStr, "MD5");
    }

    private static String encrypt(final String password, final String algorithm){
        if(password == null){
            return null;
        }
        try{
            MessageDigest md = MessageDigest.getInstance(algorithm);
            md.update(password.getBytes());

            byte[] byteArray = md.digest();
            StringBuilder strbHexString = new StringBuilder();

            for(int i = 0; i< byteArray.length;i++){
                if(Integer.toHexString(0xFF & byteArray[0]).length() == 1){
                    strbHexString.append("0").append(Integer.toHexString(0xFF & byteArray[i]));
                }else{
                    strbHexString.append(Integer.toHexString(0xFF & byteArray[i]));
                }
            }
            return strbHexString.toString();
        } catch (NoSuchAlgorithmException e) {
            return password;
        }
    }

}
