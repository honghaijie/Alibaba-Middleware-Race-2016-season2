package com.alibaba.middleware.race;

import java.io.*;
import java.nio.ByteBuffer;
import java.security.KeyException;
import java.util.*;

/**
 * Created by hahong on 2016/6/14.
 */
public class Utils {
    public static byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(x);
        return buffer.array();
    }

    public static long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.put(bytes);
        buffer.flip();//need flip
        return buffer.getLong();
    }
    public static long[] byteArrayToLongArray(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
        buffer.put(bytes);
        buffer.flip();
        long[] res = new long[bytes.length / 8];
        for (int i = 0; i < res.length; ++i) {
            res[i] = buffer.getLong();
        }
        return res;
    }
    public static byte[] convertToBytes(Object object) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(object);
            byte[] yourBytes = bos.toByteArray();
            return yourBytes;
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
            try {
                bos.close();
            } catch (IOException ex) {
                // ignore close exception
            }
        }
    }
    public static Object convertFromBytes(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInput in = null;
        try {
            in = new ObjectInputStream(bis);
            Object o = in.readObject();
            return o;
        } finally {
            try {
                bis.close();
            } catch (IOException ex) {
                // ignore close exception
            }
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
        }
    }
    public static void ClearFile(String filename) {
        try {
            PrintWriter pw = new PrintWriter(filename);
            pw.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
    public static void WriteStringsToFile(String filename, Collection<String> content) throws IOException {
        ClearFile(filename);
        File fout = new File(filename);
        FileOutputStream fos = new FileOutputStream(fout);

        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos, "UTF-8"));

        for (String s : content) {
            bw.write(s);
            bw.write('\n');
        }

        bw.close();
    }
    public static Map<String, String> ParseEntryStrToMap(String s) {
        String[] fields = s.split("\\t");
        Map<String, String> ans = new HashMap<String, String>();
        for (String kvString : fields) {
            String[] splitedKeyValuePair = kvString.split(":");
            String key = splitedKeyValuePair[0];
            String value = splitedKeyValuePair[1];
            ans.put(key, value);
        }
        return ans;
    }

    public static List<Tuple<String, String>> ParseEntryStrToList(String s) {
        String[] fields = s.split("\\t");
        List<Tuple<String, String>> ans = new ArrayList<Tuple<String, String>>();
        for (String kvString : fields) {
            String[] splitedKeyValuePair = kvString.split(":");
            String key = splitedKeyValuePair[0];
            String value = splitedKeyValuePair[1];
            ans.add(new Tuple<String, String>(key, value));
        }
        return ans;
    }

    public static long GetOrderId(byte[] s) {
        return bytesToLong(Arrays.copyOf(s, 8));
    }
    public static String GetDisk(String path) {
        return "D";
    }
    public static long hash(String string) {
        long h = 1125899906842597L; // prime
        int len = string.length();

        for (int i = 0; i < len; i++) {
            h = 31*h + string.charAt(i);
        }
        h = Math.abs(h);
        return h;
    }
}
