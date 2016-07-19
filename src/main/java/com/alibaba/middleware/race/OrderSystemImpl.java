package com.alibaba.middleware.race;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.security.KeyException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;

/**
 * Created by hahong on 2016/6/13.
 */


public class OrderSystemImpl implements OrderSystem {
    ConcurrentMap<String, Semaphore> diskSem = new ConcurrentHashMap<String, Semaphore>();

    private Map<String, Integer> orderFileIdMapper = new TreeMap<String, Integer>();
    private Map<Integer, String> orderFileIdMapperRev = new TreeMap<Integer, String>();
    private Map<String, Integer> buyerFileIdMapper = new TreeMap<String, Integer>();
    private Map<Integer, String> buyerFileIdMapperRev = new TreeMap<Integer, String>();
    private Map<String, Integer> goodFileIdMapper = new TreeMap<String, Integer>();
    private Map<Integer, String> goodFileIdMapperRev = new TreeMap<Integer, String>();

    static final int orderBlockNum = 10;
    static final int buyerBlockNum = 5;
    static final int goodBlockNum = 5;
    static final int bufferSize = 64 * 1024;
    static final int memoryOrderIndexSize = 1000;
    static final int memoryGoodIndexSize = 100;
    static final int memoryBuyerIndexSize = 100;

    List<String> unSortedOrderOrderIndexBlockFiles = new ArrayList<String>();
    List<String> sortedOrderOrderIndexBlockFiles = new ArrayList<String>();
    List<String> unSortedOrderGoodIndexBlockFiles = new ArrayList<String>();
    List<String> sortedOrderGoodIndexBlockFiles = new ArrayList<String>();
    List<String> unSortedOrderBuyerIndexBlockFiles = new ArrayList<String>();
    List<String> sortedOrderBuyerIndexBlockFiles = new ArrayList<String>();



    Map<String, BufferedOutputStream> orderOrderIndexBlockFilesOutputStreamMapper = new HashMap<String, BufferedOutputStream>();
    Map<String, BufferedOutputStream> orderGoodIndexBlockFilesOutputStreamMapper = new HashMap<String, BufferedOutputStream>();
    Map<String, BufferedOutputStream> orderBuyerIndexBlockFilesOutputStreamMapper = new HashMap<String, BufferedOutputStream>();

    Map<String, TreeMap<Long, Long>> orderOrderIndexOffset = new HashMap<String, TreeMap<Long, Long>>();
    Map<String, TreeMap<Long, Long>> orderGoodIndexOffset = new HashMap<String, TreeMap<Long, Long>>();
    Map<String, TreeMap<Tuple<Long, Long>, Long>> orderBuyerIndexOffset = new HashMap<String, TreeMap<Tuple<Long, Long>, Long>>();


    List<String> unSortedGoodGoodIndexBlockFiles = new ArrayList<String>();
    List<String> sortedGoodGoodIndexBlockFiles = new ArrayList<String>();
    List<String> unSortedBuyerBuyerIndexBlockFiles = new ArrayList<String>();
    List<String> sortedBuyerBuyerIndexBlockFiles = new ArrayList<String>();
    Map<String, BufferedOutputStream> goodGoodIndexBlockFilesOutputStreamMapper = new HashMap<String, BufferedOutputStream>();
    Map<String, BufferedOutputStream> buyerBuyerIndexBlockFilesOutputStreamMapper = new HashMap<String, BufferedOutputStream>();

    Map<String, TreeMap<Long, Long>> goodGoodIndexOffset = new HashMap<String, TreeMap<Long, Long>>();
    Map<String, TreeMap<Long, Long>> buyerBuyerIndexOffset = new HashMap<String, TreeMap<Long, Long>>();


    TreeMap<Tuple<Long, Long>, Integer> buyerBlockMapper = new TreeMap<>();

    long orderEntriesCount = 0;

    //WARNING
    long goodEntriesCount = 0;
    long buyerEntriesCount = 0;

    public OrderSystemImpl() {

    }
    private List<Tuple<Long, Long>> RandomOrder(List<String> orderFiles, int size) {
        Random rd = new Random();
        List<Tuple<Long, Long>> ans = new ArrayList<>();

        for (int orderFileId = 0; orderFileId < orderFiles.size(); ++orderFileId) {
            int left = orderFiles.size() - orderFileId;
            int curFileSample = size / left;
            size -= curFileSample;

            for (int i = 0; i < curFileSample; ++i) {
                try {
                    String filename = orderFiles.get(orderFileId);
                    File file = new File(filename);
                    long totalLength = file.length();
                    InputStreamReader isr = null;
                    FileInputStream fis = new FileInputStream(file);
                    long offset = Math.abs(rd.nextLong()) % totalLength;
                    fis.skip(offset);
                    isr = new InputStreamReader(fis, "UTF-8");
                    BufferedReader reader = new BufferedReader(isr, 1024 * 16);
                    String t = reader.readLine();
                    t = reader.readLine();
                    if (t == null) {
                        continue;
                    }
                    Map<String, String> attr = Utils.ParseEntryStrToMap(t);
                    long buyerHash = Utils.hash(attr.get("buyerid"));
                    long createTime = Long.parseLong(attr.get("createtime"));
                    ans.add(new Tuple<Long, Long>(buyerHash, createTime));
                    reader.close();
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return ans;
    }

    private long ExtractGoodOffset(List<String> goodFiles) throws IOException, KeyException, InterruptedException {
        int total = 0;
        String diskTag = Utils.GetDisk(goodFiles.get(0));
        if (!diskSem.containsKey(diskTag)) {
            diskSem.put(diskTag, new Semaphore(1));
        }
        for (int goodFileId = 0; goodFileId < goodFiles.size(); ++goodFileId) {
            String filename = goodFiles.get(goodFileId);
            File file = new File(filename);
            InputStreamReader isr = new InputStreamReader(new FileInputStream(file), "UTF-8");
            BufferedReader reader = new BufferedReader(isr, bufferSize);
            String line;
            long offset = 0;
            while (true) {
                diskSem.get(diskTag).acquire();
                line = reader.readLine();
                diskSem.get(diskTag).release();

                if (line == null) break;
                Map<String, String> attr = Utils.ParseEntryStrToMap(line);
                String goodid = attr.get("goodid");
                long goodIdHashVal = Utils.hash(goodid);

                int goodBlockId = (int)(goodIdHashVal % goodBlockNum);
                String goodIndexPath = unSortedGoodGoodIndexBlockFiles.get(goodBlockId);
                String goodIndexDiskTag = Utils.GetDisk(goodIndexPath);
                diskSem.get(goodIndexDiskTag).acquire();
                BufferedOutputStream bos = goodGoodIndexBlockFilesOutputStreamMapper.get(goodIndexPath);
                bos.write(Utils.longToBytes(goodIdHashVal));
                bos.write(Utils.longToBytes(goodFileIdMapper.get(filename)));
                bos.write(Utils.longToBytes(offset));
                diskSem.get(goodIndexDiskTag).release();

                offset += (line + "\n").getBytes(StandardCharsets.UTF_8).length;
                ++total;
            }
        }
        return total;
    }
    private long ExtractBuyerOffset(List<String> buyerFiles) throws IOException, KeyException, InterruptedException {
        int total = 0;
        String diskTag = Utils.GetDisk(buyerFiles.get(0));
        if (!diskSem.containsKey(diskTag)) {
            diskSem.put(diskTag, new Semaphore(1));
        }
        for (int buyerFileId = 0; buyerFileId < buyerFiles.size(); ++buyerFileId) {
            String filename = buyerFiles.get(buyerFileId);
            File file = new File(filename);
            InputStreamReader isr = new InputStreamReader(new FileInputStream(file), "UTF-8");
            BufferedReader reader = new BufferedReader(isr, bufferSize);
            String line;
            long offset = 0;
            while (true) {
                diskSem.get(diskTag).acquire();
                line = reader.readLine();
                diskSem.get(diskTag).release();

                if (line == null) break;
                Map<String, String> attr = Utils.ParseEntryStrToMap(line);
                String buyerid = attr.get("buyerid");
                long buyerIdHashVal = Utils.hash(buyerid);

                int buyerBlockId = (int)(buyerIdHashVal % buyerBlockNum);
                String buyerIndexPath = unSortedBuyerBuyerIndexBlockFiles.get(buyerBlockId);
                String buyerIndexDiskTag = Utils.GetDisk(buyerIndexPath);
                diskSem.get(buyerIndexDiskTag).acquire();
                BufferedOutputStream bos = buyerBuyerIndexBlockFilesOutputStreamMapper.get(buyerIndexPath);
                bos.write(Utils.longToBytes(buyerIdHashVal));
                bos.write(Utils.longToBytes(buyerFileIdMapper.get(filename)));
                bos.write(Utils.longToBytes(offset));
                diskSem.get(buyerIndexDiskTag).release();

                offset += (line + "\n").getBytes(StandardCharsets.UTF_8).length;
                ++total;
            }
        }
        return total;
    }

    private long ExtractOrderOffset(List<String> orderFiles) throws IOException, KeyException, InterruptedException {
        int total = 0;
        String diskTag = Utils.GetDisk(orderFiles.get(0));
        if (!diskSem.containsKey(diskTag)) {
            diskSem.put(diskTag, new Semaphore(1));
        }
        for (int orderFileId = 0; orderFileId < orderFiles.size(); ++orderFileId) {
            String filename = orderFiles.get(orderFileId);
            File file = new File(filename);
            InputStreamReader isr = new InputStreamReader(new FileInputStream(file), "UTF-8");
            BufferedReader reader = new BufferedReader(isr, bufferSize);
            String line;
            long offset = 0;
            while (true) {
                diskSem.get(diskTag).acquire();
                line = reader.readLine();
                diskSem.get(diskTag).release();

                if (line == null) break;
                Map<String, String> attr = Utils.ParseEntryStrToMap(line);
                long orderId = Long.parseLong(attr.get("orderid"));
                String goodid = attr.get("goodid");
                String buyerid = attr.get("buyerid");
                long createtime = Long.parseLong(attr.get("createtime"));

                if (orderId == 624813187L) {
                    int zzz = 1;
                }

                int orderBlockId = (int)(orderId % orderBlockNum);
                String orderIndexPath = unSortedOrderOrderIndexBlockFiles.get(orderBlockId);
                String orderIndexDiskTag = Utils.GetDisk(orderIndexPath);
                diskSem.get(orderIndexDiskTag).acquire();
                orderOrderIndexBlockFilesOutputStreamMapper.get(orderIndexPath).write(Utils.longToBytes(orderId));
                orderOrderIndexBlockFilesOutputStreamMapper.get(orderIndexPath).write(Utils.longToBytes(orderFileIdMapper.get(filename)));
                orderOrderIndexBlockFilesOutputStreamMapper.get(orderIndexPath).write(Utils.longToBytes(offset));
                diskSem.get(orderIndexDiskTag).release();

                long goodHashVal = Utils.hash(goodid);
                int goodBlockId = (int)((goodHashVal) % orderBlockNum);
                String goodIndexPath = unSortedOrderGoodIndexBlockFiles.get(goodBlockId);
                String goodIndexDiskTag = Utils.GetDisk(goodIndexPath);
                diskSem.get(goodIndexDiskTag).acquire();
                orderGoodIndexBlockFilesOutputStreamMapper.get(goodIndexPath).write(Utils.longToBytes(goodHashVal));
                orderGoodIndexBlockFilesOutputStreamMapper.get(goodIndexPath).write(Utils.longToBytes(orderFileIdMapper.get(filename)));
                orderGoodIndexBlockFilesOutputStreamMapper.get(goodIndexPath).write(Utils.longToBytes(offset));
                diskSem.get(goodIndexDiskTag).release();

                long buyerHashVal = Utils.hash(buyerid);
                Tuple<Long, Long> buyerIndexEntry = new Tuple<>(buyerHashVal, createtime);
                int buyerBlockId = buyerBlockMapper.floorEntry(buyerIndexEntry).getValue();
                String buyerIndexPath = unSortedOrderBuyerIndexBlockFiles.get(buyerBlockId);
                String buyerIndexDiskTag = Utils.GetDisk(buyerIndexPath);
                diskSem.get(buyerIndexDiskTag).acquire();
                orderBuyerIndexBlockFilesOutputStreamMapper.get(buyerIndexPath).write(Utils.longToBytes(buyerHashVal));
                orderBuyerIndexBlockFilesOutputStreamMapper.get(buyerIndexPath).write(Utils.longToBytes(createtime));
                orderBuyerIndexBlockFilesOutputStreamMapper.get(buyerIndexPath).write(Utils.longToBytes(orderFileIdMapper.get(filename)));
                orderBuyerIndexBlockFilesOutputStreamMapper.get(buyerIndexPath).write(Utils.longToBytes(offset));
                diskSem.get(buyerIndexDiskTag).release();



                offset += (line + "\n").getBytes(StandardCharsets.UTF_8).length;
                ++total;
            }
        }
        return total;
    }
    private Map<String, TreeMap<Long, Long>> SortOffset(List<String> unOrderedFiles, List<String> orderedFiles, long ratio) throws IOException, KeyException, InterruptedException {
        Map<String, TreeMap<Long, Long>> res = new HashMap<>();
        for (int i = 0; i < unOrderedFiles.size(); ++i) {
            String unOrderedFilename = unOrderedFiles.get(i);
            String orderedFilename = orderedFiles.get(i);
            File file = new File(unOrderedFilename);
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file), bufferSize);
            int entryLength = 24;
            long offset = 0;
            //Map<Long, Tuple<Long, Long>> indexMapper = new TreeMap<Long, Tuple<Long, Long>>();
            List<Tuple<Long, Tuple<Long, Long>>> indexList = new ArrayList<>();
            while (true) {
                byte[] entryBytes = new byte[entryLength];
                int len = bis.read(entryBytes);
                if (len == -1) break;
                long[] e = Utils.byteArrayToLongArray(entryBytes);
                indexList.add(new Tuple<Long, Tuple<Long, Long>>(e[0], new Tuple<Long, Long>(e[1], e[2])));
            }
            bis.close();
            Collections.sort(indexList, new Comparator<Tuple<Long, Tuple<Long, Long>>>() {
                @Override
                public int compare(Tuple<Long, Tuple<Long, Long>> o1, Tuple<Long, Tuple<Long, Long>> o2) {
                    return o1.x.compareTo(o2.x);
                }
            });
            TreeMap<Long, Long> currentMap = new TreeMap<>();
            BufferedOutputStream fos = new BufferedOutputStream(new FileOutputStream(orderedFilename), bufferSize);
            int cnt = 0;
            for (int idx = 0; idx < indexList.size(); ++idx) {
                Tuple<Long, Tuple<Long, Long>> e = indexList.get(idx);

                fos.write(Utils.longToBytes(e.x));
                fos.write(Utils.longToBytes(e.y.x));
                fos.write(Utils.longToBytes(e.y.y));
                if (idx == 0 || !indexList.get(idx - 1).x.equals(e.x)) {
                    ++cnt;
                    if (cnt % ratio == 0) {
                        currentMap.put(e.x, offset);
                    }
                }
                offset += entryLength;
            }
            currentMap.put(Long.MIN_VALUE, 0L);
            currentMap.put(Long.MAX_VALUE, offset);
            res.put(orderedFilename, currentMap);
            fos.close();
        }
        return res;
    }
    private Map<String, TreeMap<Tuple<Long, Long>, Long>> SortBuyerOffset(List<String> unOrderedFiles, List<String> orderedFiles, long ratio) throws IOException, KeyException, InterruptedException {
        Map<String, TreeMap<Tuple<Long, Long>, Long>> res = new HashMap<>();
        for (int i = 0; i < unOrderedFiles.size(); ++i) {
            String unOrderedFilename = unOrderedFiles.get(i);
            String orderedFilename = orderedFiles.get(i);
            File file = new File(unOrderedFilename);
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file), bufferSize);
            int entryLength = 32;
            long offset = 0;
            //Map<Long, Tuple<Long, Long>> indexMapper = new TreeMap<Long, Tuple<Long, Long>>();
            List<Tuple<Tuple<Long, Long>, Tuple<Long, Long>>> indexList = new ArrayList<>();
            while (true) {
                byte[] entryBytes = new byte[entryLength];
                int len = bis.read(entryBytes);
                if (len == -1) break;
                long[] e = Utils.byteArrayToLongArray(entryBytes);
                indexList.add(new Tuple<Tuple<Long, Long>, Tuple<Long, Long>>(new Tuple<Long, Long>(e[0], e[1]), new Tuple<Long, Long>(e[2], e[3])));
            }
            bis.close();
            Collections.sort(indexList);
            TreeMap<Tuple<Long, Long>, Long> currentMap = new TreeMap<>();
            BufferedOutputStream fos = new BufferedOutputStream(new FileOutputStream(orderedFilename), bufferSize);
            int cnt = 0;
            for (int idx = 0; idx < indexList.size(); ++idx) {
                Tuple<Tuple<Long, Long>, Tuple<Long, Long>> e = indexList.get(idx);

                fos.write(Utils.longToBytes(e.x.x));
                fos.write(Utils.longToBytes(e.x.y));
                fos.write(Utils.longToBytes(e.y.x));
                fos.write(Utils.longToBytes(e.y.y));
                ++cnt;
                if (cnt % ratio == 0) {
                    currentMap.put(e.x, offset);
                }
                offset += entryLength;
            }
            currentMap.put(new Tuple<Long, Long>(Long.MIN_VALUE, Long.MIN_VALUE), 0L);
            currentMap.put(new Tuple<Long, Long>(Long.MAX_VALUE, Long.MAX_VALUE), offset);
            res.put(orderedFilename, currentMap);
            fos.close();
        }
        return res;
    }
    private void PreProcessOrders(List<String> orderFiles, List<String> buyerFiles, List<String> goodFiles, List<String> storeFolders) throws IOException, KeyException, InterruptedException {
        //goodRawFileOffset = ExtractGoodOffset(goodFiles);
        //buyerRawFileOffset = ExtractBuyerOffset(buyerFiles);

        for (String orderFile : orderFiles) {
            orderFileIdMapperRev.put(orderFileIdMapperRev.size(), orderFile);
            orderFileIdMapper.put(orderFile, orderFileIdMapper.size());
        }
        for (String buyerFile : buyerFiles) {
            buyerFileIdMapperRev.put(buyerFileIdMapperRev.size(), buyerFile);
            buyerFileIdMapper.put(buyerFile, buyerFileIdMapper.size());
        }
        for (String goodFile : goodFiles) {
            goodFileIdMapperRev.put(goodFileIdMapperRev.size(), goodFile);
            goodFileIdMapper.put(goodFile, goodFileIdMapper.size());
        }

        List<Tuple<Long, Long>> randomBuyerEntries = RandomOrder(orderFiles, orderBlockNum - 1);
        Random rd = new Random();
        for (int i = 0; i < orderBlockNum; ++i) {
            String currentStoreFolder = storeFolders.get(rd.nextInt(storeFolders.size()));
            String unSortedOrderPath = currentStoreFolder + "\\" + UUID.randomUUID().toString();
            String sortedOrderPath = currentStoreFolder + "\\" + UUID.randomUUID().toString();
            unSortedOrderOrderIndexBlockFiles.add(unSortedOrderPath);
            sortedOrderOrderIndexBlockFiles.add(sortedOrderPath);
            orderOrderIndexBlockFilesOutputStreamMapper.put(unSortedOrderPath, new BufferedOutputStream(new FileOutputStream(unSortedOrderPath), bufferSize));
        }


        for (int i = 0; i < orderBlockNum; ++i) {
            String currentStoreFolder = storeFolders.get(rd.nextInt(storeFolders.size()));
            String unSortedGoodPath = currentStoreFolder + "\\" + UUID.randomUUID().toString();
            String sortedGoodPath = currentStoreFolder + "\\" + UUID.randomUUID().toString();
            unSortedOrderGoodIndexBlockFiles.add(unSortedGoodPath);
            sortedOrderGoodIndexBlockFiles.add(sortedGoodPath);
            orderGoodIndexBlockFilesOutputStreamMapper.put(unSortedGoodPath, new BufferedOutputStream(new FileOutputStream(unSortedGoodPath), bufferSize));
        }

        buyerBlockMapper.put(new Tuple<Long, Long>(-1L, -1L), 0);
        for (Tuple<Long, Long> e : randomBuyerEntries) {
            buyerBlockMapper.put(e, buyerBlockMapper.size());
        }
        for (int i = 0; i < buyerBlockMapper.size(); ++i) {
            String currentStoreFolder = storeFolders.get(rd.nextInt(storeFolders.size()));
            String unSortedBuyerPath = currentStoreFolder + "\\" + UUID.randomUUID().toString();
            String sortedBuyerPath = currentStoreFolder + "\\" + UUID.randomUUID().toString();
            unSortedOrderBuyerIndexBlockFiles.add(unSortedBuyerPath);
            sortedOrderBuyerIndexBlockFiles.add(sortedBuyerPath);
            orderBuyerIndexBlockFilesOutputStreamMapper.put(unSortedBuyerPath, new BufferedOutputStream(new FileOutputStream(unSortedBuyerPath), bufferSize));
        }

        for (int i = 0; i < buyerBlockNum; ++i) {
            String currentStoreFolder = storeFolders.get(rd.nextInt(storeFolders.size()));
            String unSortedBuyerPath = currentStoreFolder + "\\" + UUID.randomUUID().toString();
            String sortedBuyerPath = currentStoreFolder + "\\" + UUID.randomUUID().toString();
            unSortedBuyerBuyerIndexBlockFiles.add(unSortedBuyerPath);
            sortedBuyerBuyerIndexBlockFiles.add(sortedBuyerPath);
            buyerBuyerIndexBlockFilesOutputStreamMapper.put(unSortedBuyerPath, new BufferedOutputStream(new FileOutputStream(unSortedBuyerPath), bufferSize));
        }
        for (int i = 0; i < goodBlockNum; ++i) {
            String currentStoreFolder = storeFolders.get(rd.nextInt(storeFolders.size()));
            String unSortedGoodPath = currentStoreFolder + "\\" + UUID.randomUUID().toString();
            String sortedGoodPath = currentStoreFolder + "\\" + UUID.randomUUID().toString();
            unSortedGoodGoodIndexBlockFiles.add(unSortedGoodPath);
            sortedGoodGoodIndexBlockFiles.add(sortedGoodPath);
            goodGoodIndexBlockFilesOutputStreamMapper.put(unSortedGoodPath, new BufferedOutputStream(new FileOutputStream(unSortedGoodPath), bufferSize));
        }

        orderEntriesCount = ExtractOrderOffset(orderFiles);
        buyerEntriesCount = ExtractBuyerOffset(buyerFiles);
        goodEntriesCount = ExtractGoodOffset(goodFiles);
        for (BufferedOutputStream s : orderOrderIndexBlockFilesOutputStreamMapper.values()) {
            s.close();
        }
        for (BufferedOutputStream s : orderGoodIndexBlockFilesOutputStreamMapper.values()) {
            s.close();
        }
        for (BufferedOutputStream s : orderBuyerIndexBlockFilesOutputStreamMapper.values()) {
            s.close();
        }
        for (BufferedOutputStream s : buyerBuyerIndexBlockFilesOutputStreamMapper.values()) {
            s.close();
        }
        for (BufferedOutputStream s : goodGoodIndexBlockFilesOutputStreamMapper.values()) {
            s.close();
        }
        long orderOrderRatio = orderEntriesCount / memoryOrderIndexSize;
        orderGoodIndexOffset.putAll(SortOffset(unSortedOrderGoodIndexBlockFiles, sortedOrderGoodIndexBlockFiles, orderOrderRatio));

        long orderGoodRatio = goodEntriesCount / memoryGoodIndexSize;
        orderOrderIndexOffset.putAll(SortOffset(unSortedOrderOrderIndexBlockFiles, sortedOrderOrderIndexBlockFiles, orderGoodRatio));

        long orderBuyerRatio = orderEntriesCount / memoryBuyerIndexSize;
        orderBuyerIndexOffset.putAll(SortBuyerOffset(unSortedOrderBuyerIndexBlockFiles, sortedOrderBuyerIndexBlockFiles, orderBuyerRatio));

        long buyerBuyerRatio = buyerEntriesCount / memoryBuyerIndexSize;
        buyerBuyerIndexOffset.putAll(SortOffset(unSortedBuyerBuyerIndexBlockFiles, sortedBuyerBuyerIndexBlockFiles, buyerBuyerRatio));

        long goodGoodRatio = orderEntriesCount / memoryGoodIndexSize;
        goodGoodIndexOffset.putAll(SortOffset(unSortedGoodGoodIndexBlockFiles, sortedGoodGoodIndexBlockFiles, goodGoodRatio));

    }
    private List<String> QueryEntryById(long id, long blockNum, Map<String, TreeMap<Long, Long>> indexOffset, List<String> sortedIndexBlockFiles, Map<Integer, String> fileIdMapperRev) {
        int blockId = (int)(id % blockNum);
        TreeMap<Long, Long> blockIndex = indexOffset.get(sortedIndexBlockFiles.get(blockId));
        long offset = blockIndex.floorEntry(id).getValue();
        int len = (int)(blockIndex.higherEntry(id).getValue() - offset);

        try {
            File file = new File(sortedIndexBlockFiles.get(blockId));
            ByteBuffer bb = ByteBuffer.allocate(len);
            FileChannel.open(file.toPath()).position(offset).read(bb);

            byte[] buf = bb.array();
            long[] ls = Utils.byteArrayToLongArray(buf);
            List<Tuple<Long, Long>> r = new ArrayList<>();
            for (int i = 0; i < ls.length; i += 3) {
                if (ls[i] == id) {
                    long fileId = ls[i + 1];
                    long rawOffset = ls[i + 2];
                    r.add(new Tuple<Long, Long>(fileId, rawOffset));
                }
            }
            List<String> ans = new ArrayList<>();
            for (Tuple<Long, Long> item : r) {
                long fileId = item.x;
                long rawOffset = item.y;
                File f = new File(fileIdMapperRev.get((int) fileId));
                FileInputStream fis = new FileInputStream(f);
                fis.skip(rawOffset);
                InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
                BufferedReader reader = new BufferedReader(isr, 1024);
                ans.add(reader.readLine());

                reader.close();
            }
            return ans;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        }
        return null;
    }

    private List<String> QueryOrderByBuyer(long buyerHashVal, long from, long to, Map<String, TreeMap<Tuple<Long, Long>, Long>> indexOffset, List<String> sortedIndexBlockFiles) {
        Tuple<Long, Long> buyerIndexEntryLowerBound = new Tuple<>(buyerHashVal, from);
        Tuple<Long, Long> buyerIndexEntryUpperBound = new Tuple<>(buyerHashVal, to);
        int blockId = buyerBlockMapper.floorEntry(buyerIndexEntryLowerBound).getValue();

        TreeMap<Tuple<Long, Long>, Long> blockIndex = indexOffset.get(sortedIndexBlockFiles.get(blockId));
        long offset = blockIndex.floorEntry(buyerIndexEntryLowerBound).getValue();
        int len = (int)(blockIndex.ceilingEntry(buyerIndexEntryUpperBound).getValue() - offset);

        try {
            File file = new File(sortedIndexBlockFiles.get(blockId));
            ByteBuffer bb = ByteBuffer.allocate(len);
            FileChannel.open(file.toPath()).position(offset).read(bb);

            byte[] buf = bb.array();
            long[] ls = Utils.byteArrayToLongArray(buf);
            List<Tuple<Long, Long>> r = new ArrayList<>();
            for (int i = 0; i < ls.length; i += 4) {
                if (ls[i] == buyerHashVal && ls[i + 1] >= from && ls[i + 1] < to) {
                    long fileId = ls[i + 2];
                    long rawOffset = ls[i + 3];
                    r.add(new Tuple<Long, Long>(fileId, rawOffset));
                }
            }
            List<String> ans = new ArrayList<>();
            for (Tuple<Long, Long> item : r) {
                long fileId = item.x;
                long rawOffset = item.y;
                File f = new File(orderFileIdMapperRev.get((int) fileId));
                FileInputStream fis = new FileInputStream(f);
                fis.skip(rawOffset);
                InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
                BufferedReader reader = new BufferedReader(isr, 1024);
                ans.add(reader.readLine());

                reader.close();
            }
            return ans;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        }
        return null;
    }

    private String QueryBuyerByBuyer(String buyerid) {
        List<String> ans = QueryEntryById(Utils.hash(buyerid), buyerBlockNum, buyerBuyerIndexOffset, sortedBuyerBuyerIndexBlockFiles, buyerFileIdMapperRev);
        return ans.get(0);
    }

    private String QueryGoodByGood(String goodid) {
        List<String> ans = QueryEntryById(Utils.hash(goodid), goodBlockNum, goodGoodIndexOffset, sortedGoodGoodIndexBlockFiles, goodFileIdMapperRev);
        return ans.get(0);
    }

    @Override
    public void construct(Collection<String> orderFiles, Collection<String> buyerFiles, Collection<String> goodFiles, Collection<String> storeFolders) throws IOException, InterruptedException {
        try {
            PreProcessOrders(new ArrayList<String>(orderFiles), new ArrayList<String>(buyerFiles), new ArrayList<String>(goodFiles), new ArrayList<String>(storeFolders));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Result queryOrder(long orderId, Collection<String> keys) {
        synchronized (this) {
            List<String> ans = QueryEntryById(orderId, orderBlockNum, orderOrderIndexOffset, sortedOrderOrderIndexBlockFiles, orderFileIdMapperRev);
            Set<String> attrs = null;
            if (keys != null) {
                attrs = new HashSet<>(keys);
            }
            if (ans.isEmpty()) return null;
            String r = ans.get(0);
            Map<String, String> orderLs = Utils.ParseEntryStrToMap(r);
            String buyerStr = QueryBuyerByBuyer(orderLs.get("buyerid"));
            String goodStr = QueryGoodByGood(orderLs.get("goodid"));
            orderLs.putAll(Utils.ParseEntryStrToMap(buyerStr));
            orderLs.putAll(Utils.ParseEntryStrToMap(goodStr));

            Map<String, String> rt = new HashMap<>();
            for (Map.Entry<String, String> t : orderLs.entrySet()) {
                if (t.getKey().equals("orderid") || attrs == null || attrs.contains(t.getKey())) {
                    rt.put(t.getKey(), t.getValue());
                }
            }


            return new QueryResult(rt);
        }
    }

    @Override
    public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime, String buyerid) {
        synchronized (this) {
            List<String> ans = QueryOrderByBuyer(Utils.hash(buyerid), startTime, endTime, orderBuyerIndexOffset, sortedOrderBuyerIndexBlockFiles);
            Map<String, String> buyerInfo = Utils.ParseEntryStrToMap(QueryBuyerByBuyer(buyerid));
            List<Result> rr = new ArrayList<>();
            if (ans.isEmpty()) return rr.iterator();

            for (String r : ans) {
                Map<String, String> ls = Utils.ParseEntryStrToMap(r);
                Map<String, String> goodInfo = Utils.ParseEntryStrToMap(QueryGoodByGood(ls.get("goodid")));
                ls.putAll(buyerInfo);
                ls.putAll(goodInfo);
                rr.add(new QueryResult(ls));
            }
            Collections.sort(rr, new Comparator<Result>() {
                @Override
                public int compare(Result o1, Result o2) {
                    try {
                        return -((Long) o1.get("createtime").valueAsLong()).compareTo(o2.get("createtime").valueAsLong());
                    } catch (TypeException e) {
                        e.printStackTrace();
                    }
                    return 0;
                }
            });
            return rr.iterator();
        }
    }

    @Override
    public Iterator<Result> queryOrdersBySaler(String salerid, String goodid, Collection<String> keys) {
        synchronized (this) {
            List<String> ans = QueryEntryById(Utils.hash(goodid), orderBlockNum, orderGoodIndexOffset, sortedOrderGoodIndexBlockFiles, orderFileIdMapperRev);
            Set<String> attrs = null;
            if (keys != null) {
                attrs = new HashSet<>(keys);
            } else {
                attrs = null;
            }
            List<Result> rr = new ArrayList<>();
            if (ans.isEmpty()) return rr.iterator();
            for (String r : ans) {
                Map<String, String> orderLs = Utils.ParseEntryStrToMap(r);
                String buyerStr = QueryBuyerByBuyer(orderLs.get("buyerid"));
                String goodStr = QueryGoodByGood(orderLs.get("goodid"));
                orderLs.putAll(Utils.ParseEntryStrToMap(buyerStr));
                orderLs.putAll(Utils.ParseEntryStrToMap(goodStr));

                Map<String, String> rt = new HashMap<>();
                for (Map.Entry<String, String> t : orderLs.entrySet()) {
                    if (t.getKey().equals("orderid") || attrs == null || attrs.contains(t.getKey())) {
                        rt.put(t.getKey(), t.getValue());
                    }
                }
                rr.add(new QueryResult(rt));
            }
            Collections.sort(rr, new Comparator<Result>() {
                @Override
                public int compare(Result o1, Result o2) {
                    try {
                        return ((Long) o1.get("orderid").valueAsLong()).compareTo(o2.get("orderid").valueAsLong());
                    } catch (TypeException e) {
                        e.printStackTrace();
                    }
                    return 0;
                }
            });
            return rr.iterator();
        }
    }

    @Override
    public KeyValue sumOrdersByGood(String goodid, String key) {
        synchronized (this) {
            //List<String> ans = QueryEntryById(Utils.hash(goodid), orderBlockNum, orderGoodIndexOffset, sortedOrderGoodIndexBlockFiles, orderFileIdMapperRev);
            Iterator<Result> ans = queryOrdersBySaler("", goodid, Arrays.asList(key));
            if (!ans.hasNext()) return null;
            long longSum = 0L;
            double doubleSum = 0.0;
            boolean isDouble = false;
            try {
                while (ans.hasNext()) {
                    Result pr = ans.next();
                    String t = pr.get(key).valueAsString();
                    if (t == null) {
                        continue;
                    }
                    double d = Double.parseDouble(t);
                    if (isDouble) {
                        doubleSum += d;
                    } else if (t.contains(".")) {
                        isDouble = true;
                        doubleSum = longSum;
                        doubleSum += d;
                    } else {
                        longSum += Long.parseLong(t);
                    }

                }
            } catch (Exception e) {
                longSum = 0;
                doubleSum = 0.0;
            }
            QueryKeyValue kv = new QueryKeyValue(key, isDouble ? ((Double) doubleSum).toString() : ((Long) longSum).toString());
            return kv;
        }
    }

    public static void main(String[] args) throws InterruptedException, KeyException, IOException, TypeException {
        long startTime = System.currentTimeMillis();
        List<String> orderFiles = Arrays.asList("D:\\middleware-data\\order_records.txt");
        List<String> goodFiles = Arrays.asList("D:\\middleware-data\\good_records.txt");
        List<String> buyerFiles = Arrays.asList("D:\\middleware-data\\buyer_records.txt");
        List<String> storeFolders = Arrays.asList("D:\\middleware-data");

        OrderSystemImpl osi = new OrderSystemImpl();

        osi.construct(orderFiles, buyerFiles, goodFiles, storeFolders);

        String s = "aliyun_694d9233-ca7a-436d-a235-9412aac0c31f";
        String buyerid = "tb_9a20ec63-cc2e-4056-b8c5-238ed94f2ec6";
        //List<String> ans = osi.QueryOrderByBuyer(Utils.hash(buyerid), 1470668508, 5463667280L, osi.orderBuyerIndexOffset, osi.sortedOrderBuyerIndexBlockFiles);
/*
        List<String> ans = Arrays.asList(osi.QueryBuyerByBuyer("tb_171da9af-8527-45cc-97f9-e4fb6da4aee6"));
        System.out.println(ans.size());
        for (String e : ans) {
            System.out.println(e);
        }
*/
        Iterator<Result> ans = Arrays.asList(osi.queryOrder(3008769L, null)).iterator();
        while (ans.hasNext()) {
            Result r = ans.next();
            for (KeyValue k : r.getAll()) {
                System.out.print(k.key() + ":" + k.valueAsString() + ", ");
            }
            System.out.println();
        }
        System.out.printf("Time: %f\n", (System.currentTimeMillis() - startTime) / 1000.0);
    }
}
