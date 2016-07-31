package com.alibaba.middleware.race;

import java.io.*;
import java.nio.ByteBuffer;
import java.security.KeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
    public static byte[] longToBytes(long x, long y) {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(x);
        buffer.putLong(y);
        return buffer.array();
    }
    public static byte[] longToBytes(long x, long y, long z) {
        ByteBuffer buffer = ByteBuffer.allocate(24);
        buffer.putLong(x);
        buffer.putLong(y);
        buffer.putLong(z);
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

    public static long[] byteArrayToLongArray(ByteBuffer bf) {
        bf.position(0);
        long[] res = new long[bf.remaining() / 8];
        for (int i = 0; i < res.length; ++i) {
            res[i] = bf.getLong();
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
        HashMap<String, String> ans = new HashMap<String, String>();
        int from = 0;
        while (true) {
            int spIdx = s.indexOf(':', from);
            int nxIdx = s.indexOf('\t', spIdx);
            if (nxIdx == -1) nxIdx = s.length();
            String key = s.substring(from, spIdx);
            String value = s.substring(spIdx + 1, nxIdx);
            ans.put(key, value);
            if (nxIdx == s.length()) break;
            from = nxIdx + 1;
        }
        return ans;
    }

    public static String GetAttribute(String s, String qkey) {
        int from = 0;
        while (true) {
            int spIdx = s.indexOf(':', from);
            int nxIdx = s.indexOf('\t', spIdx);
            if (nxIdx == -1) nxIdx = s.length();
            String key = s.substring(from, spIdx);
            String value = s.substring(spIdx + 1, nxIdx);
            if (key.equals(qkey)) {
                return value;
            }
            if (nxIdx == s.length()) break;
            from = nxIdx + 1;
        }
        return null;
    }

    public static int GetAttribute(char[] s, int from, int to, String qkey, char[] keyBuf, char[] res) {
        int keyCnt = 0;
        int i = from;
        int resLength = 0;
        while (i < to) {
            if (s[i] == ':') {
                resLength = 0;
                for(;;) {
                    ++i;
                    if (i == to || s[i] == '\t') {
                        boolean match = true;
                        if (qkey.length() == keyCnt) {
                            for (int k = 0; k < keyCnt; ++k) {
                                if (keyBuf[k] != qkey.charAt(k)) {
                                    match = false;
                                    break;
                                }
                            }
                        } else {
                            match = false;
                        }
                        if (match) {
                            return resLength;
                        } else {
                            ++i;
                            keyCnt = 0;
                            break;
                        }
                    }
                    res[resLength++] = s[i];
                }
            }else {
                keyBuf[keyCnt++] = s[i++];
            }
        }
        return -1;
    }

    public static void ScanAttribute(char[] s, int from, int to, char[] keyBuf, int[] arr, int type) {
        int keyCnt = 0;
        int i = from;
        while (i < to) {
            if (s[i] == ':') {
                for(;;) {
                    ++i;
                    if (i == to || s[i] == '\t') {
                        arr[(int)(Utils.hash(keyBuf, 0, keyCnt) % arr.length)] = type;
                        ++i;
                        keyCnt = 0;
                        break;

                    }
                }
            }else {
                keyBuf[keyCnt++] = s[i++];
            }
        }
    }

    public static String GetDisk(String path) {
        int pos = path.indexOf('/', 1);
        if (pos != -1) {
            return path.substring(1, pos);
        } else {
            pos = path.indexOf('\\');
            return path.substring(0, pos);
        }
    }
    public static List<List<String>> GroupByDisk(List<String> files) {
        Map<String, List<String>> ans = new TreeMap<>();
        for (String orderFile : files) {
            String diskTag = Utils.GetDisk(orderFile);
            List<String> t = ans.get(diskTag);
            if (t == null) {
                t = new ArrayList<String>();
                ans.put(diskTag, t);
            }
            t.add(orderFile);
        }
        return new ArrayList<>(ans.values());
    }
    public static List<String> GetDisks(List<String> files) {
        Set<String> s = new HashSet<>();
        for (String f : files) {
            s.add(GetDisk(f));
        }
        return new ArrayList<>(s);
    }
    public static List<String> filterByDisk(List<String> files, String tag) {
        List<String> ans = new ArrayList<>();
        for (String file : files) {
            if (GetDisk(file).equals(tag)) {
                ans.add(file);
            }
        }
        return ans;
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
    public static long hash(char[] s, int from, int to) {
        long h = 1125899906842597L; // prime

        for (int i = from; i < to; i++) {
            h = 31*h + s[i];
        }
        h = Math.abs(h);
        return h;
    }
    public static long ZipFileIdAndOffset(long fileId, long offset) {
        return (fileId << 45) + offset;
    }
    public static Tuple<Long, Long> UnZipFileIdAndOffset(long m) {
        long a = m >> 45;
        long b = m - (a << 45);
        return new Tuple<>(a, b);
    }
    public static int UTF8Length(CharSequence sequence) {
        int count = 0;
        for (int i = 0, len = sequence.length(); i < len; i++) {
            char ch = sequence.charAt(i);
            if (ch <= 0x7F) {
                count++;
            } else if (ch <= 0x7FF) {
                count += 2;
            } else if (Character.isHighSurrogate(ch)) {
                count += 4;
                ++i;
            } else {
                count += 3;
            }
        }
        return count;

    }
    public static int UTF8Length(char[] c, int from, int to) {
        int count = 0;
        for (int i = from; i < to; ++i) {
            char ch = c[i];
            if (ch <= 0x7F) {
                count++;
            } else if (ch <= 0x7FF) {
                count += 2;
            } else if (Character.isHighSurrogate(ch)) {
                count += 4;
                ++i;
            } else {
                count += 3;
            }
        }
        return count;

    }
    public static List<List<String>> SplitFiles(List<String> files, int num) {
        List<List<String>> ans = new ArrayList<>();
        List<Long> lengths = new ArrayList<>();
        for (int i = 0; i < num; ++i) {
            ans.add(new ArrayList<String>());
            lengths.add(0L);
        }
        for (String file : files) {
            File f = new File(file);
            long len = f.length();
            int pos = 0;
            for (int i = 0; i < num; ++i) {
                if (lengths.get(i) < lengths.get(pos)) {
                    pos = i;
                }
            }
            ans.get(pos).add(file);
            lengths.set(pos, lengths.get(pos) + len);
        }
        return ans;
    }
    public static List<List<String>> SplitFiles(List<String> files1, List<String> files2, int num) {
        List<List<String>> ans = new ArrayList<>();
        List<Long> lengths = new ArrayList<>();
        for (int i = 0; i < num; ++i) {
            ans.add(new ArrayList<String>());
            lengths.add(0L);
        }
        for (String file : files1) {
            File f = new File(file);
            long len = f.length();
            int pos = 0;
            for (int i = 0; i < num; ++i) {
                if (lengths.get(i) < lengths.get(pos)) {
                    pos = i;
                }
            }
            ans.get(pos).add(file);
            lengths.set(pos, lengths.get(pos) + len);
        }
        return ans;
    }
    public static long ParseLong(char[] s, int from, int to) {
        long ans = 0;
        boolean neg = false;
        if (s[from] == '+') ++from;
        if (s[from] == '-') {
            ++from;
            neg = true;
        }
        for (;from < to; ++from) {
            ans = ans * 10 + s[from] - '0';
        }
        return neg ? -ans : ans;
    }
    public static void QuickSort(long[] a, int l, int r) {
        int i = l, j = r;
        long m = a[(i / 2 + j / 2) / 2 * 2];
        do {
            while (m > a[i]) i += 2;
            while (m < a[j]) j -= 2;
            if (i <= j) {
                long t = a[i];
                a[i] = a[j];
                a[j] = t;
                t = a[i + 1];
                a[i + 1] = a[j + 1];
                a[j + 1] = t;

                i += 2;
                j -= 2;
            }
        } while (i <= j);
        if (j > l) QuickSort(a, l, j);
        if (i < r) QuickSort(a, i, r);
    }
}
