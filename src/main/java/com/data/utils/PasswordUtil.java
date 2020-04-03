package com.data.utils;

import java.util.UUID;

public abstract class PasswordUtil {


    public static SaltPassword createSaltPassword(String password){
        String salt = UUID.randomUUID().toString().replaceAll("-","");
        return createSaltPassword(password,salt);
    }

    public static SaltPassword createSaltPassword(String password, String salt){
        String p = concatSaltPassword(password, salt);
        String md5 = MD5Util.encryptMD5(p);
        return new SaltPassword(password,salt, md5);
    }

    private static String concatSaltPassword(String password, String salt) {
        StringBuilder sb = new StringBuilder();
        sb.append(password == null ? "": password);
        sb.append("{");
        sb.append(salt);
        sb.append("}");
        return sb.toString();
    }

    public static void main(String[] args) {
        SaltPassword pass = createSaltPassword("hazq@123","f04d0ddfb9904fbbacbc5df3e2b21977");
        System.out.println("pass="+pass.getPassword() +"\nsalt=" + pass.getSalt() +
                "\nEncrypt="+pass.getEncryptPassword());
    }

    public static class SaltPassword {

        private final String salt;

        private final String password;

        private final String encryptPassword;


        public SaltPassword(String password, String salt, String encryptPassword) {
            this.password = password;
            this.salt = salt;
            this.encryptPassword = encryptPassword;
        }

        public String getSalt() {
            return salt;
        }

        public String getPassword() {
            return password;
        }

        public String getEncryptPassword() {
            return encryptPassword;
        }
    }


}
