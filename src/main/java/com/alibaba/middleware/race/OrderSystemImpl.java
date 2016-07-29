package com.alibaba.middleware.race;

import com.alibaba.middleware.race.diskio.DiskBytesWriter;
import com.alibaba.middleware.race.diskio.DiskStringReader;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.KeyException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by hahong on 2016/6/13.
 */


public class OrderSystemImpl implements OrderSystem {
    private List<String> disks = new ArrayList<>();
    private Object constructFinishNotifier = new Object();
    private boolean constructFinish = false;

    private Map<String, DiskBytesWriter> diskWriterMap = new HashMap<>();

    private Map<String, Integer> fileIdMapper = new TreeMap<String, Integer>();
    private Map<Integer, String> fileIdMapperRev = new TreeMap<Integer, String>();

    private Map<String, BigMappedByteBuffer> mbbMap = new HashMap<>(10000);

    private SimpleCache rawDataCache = new SimpleCache(49999);


    static final int orderBlockNum = 500;
    static final int buyerBlockNum = 50;
    static final int goodBlockNum = 50;
    static final int bufferSize = 256 * 1024;
    static final int memoryOrderOrderIndexSize = 2000000;
    static final int memoryOrderGoodIndexSize = 40000;
    static final int memoryOrderBuyerIndexSize = 2000000;

    static final int memoryBuyerBuyerIndexSize = 40000;
    static final int memoryGoodGoodIndexSize = 40000;

    List<String> unSortedOrderOrderIndexBlockFiles = new ArrayList<String>();
    List<String> sortedOrderOrderIndexBlockFiles = new ArrayList<String>();
    List<String> unSortedOrderGoodIndexBlockFiles = new ArrayList<String>();
    List<String> sortedOrderGoodIndexBlockFiles = new ArrayList<String>();
    List<String> unSortedOrderBuyerIndexBlockFiles = new ArrayList<String>();
    List<String> sortedOrderBuyerIndexBlockFiles = new ArrayList<String>();


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

    Map<String, String> attrToTable = new HashMap<>(10000);
    AtomicLong orderEntriesCount = new AtomicLong(0L);

    //WARNING
    AtomicLong goodEntriesCount = new AtomicLong(0L);
    AtomicLong buyerEntriesCount = new AtomicLong(0L);

    public OrderSystemImpl() {

    }
    private List<Tuple<Long, Long>> RandomOrder(List<String> orderFiles, int size) {
        Random rd = new Random(123);
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
        final AtomicLong total = new AtomicLong(0L);
        int threadNumber = 16;
        Thread ths[] = new Thread[threadNumber];
        List<List<String>> threadFiles = Utils.SplitFiles(goodFiles, threadNumber);

        for (int i = 0; i < threadNumber; ++i) {
            final DiskStringReader reader = new DiskStringReader(threadFiles.get(i));
            ths[i] = new Thread() {
                public void run() {
                    Map<String, String> threadAttrTable = new HashMap<>();
                    long threadTotal = 0L;
                    while (true) {
                        DiskStringReader.ReadEntry tp = reader.readLineAndFileName();
                        if (tp == null) break;
                        String line = tp.content;
                        String filename = tp.filename;
                        long offset = tp.offset;
                        Map<String, String> attr = Utils.ParseEntryStrToMap(line);
                        String goodid = attr.get("goodid");
                        for (Map.Entry<String, String> t : attr.entrySet()) {
                            threadAttrTable.put(t.getKey(), Config.GoodTable);
                        }
                        long goodIdHashVal = Utils.hash(goodid);

                        int goodBlockId = (int) (goodIdHashVal % goodBlockNum);
                        String goodIndexPath = unSortedGoodGoodIndexBlockFiles.get(goodBlockId);

                        BufferedOutputStream bos = goodGoodIndexBlockFilesOutputStreamMapper.get(goodIndexPath);
                        synchronized (bos) {
                            try {
                                bos.write(Utils.longToBytes(goodIdHashVal));
                                bos.write(Utils.longToBytes(Utils.ZipFileIdAndOffset(fileIdMapper.get(filename), offset)));
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        ++threadTotal;
                    }
                    total.addAndGet(threadTotal);
                    synchronized (attrToTable) {
                        attrToTable.putAll(threadAttrTable);
                    }
                }
            };
            ths[i].start();
        }
        for (int i = 0; i < threadNumber; ++i) {
            ths[i].join();
        }
        return total.get();
    }
    private long ExtractBuyerOffset(List<String> buyerFiles) throws IOException, KeyException, InterruptedException {
        final AtomicLong total = new AtomicLong(0L);
        int threadNumber = 16;
        Thread ths[] = new Thread[threadNumber];
        List<List<String>> threadFiles = Utils.SplitFiles(buyerFiles, threadNumber);

        for (int i = 0; i < threadNumber; ++i) {
            final DiskStringReader reader = new DiskStringReader(threadFiles.get(i));
            ths[i] = new Thread() {
                public void run() {
                    Map<String, String> threadAttrTable = new HashMap<>();
                    long threadTotal = 0L;
                    String filename = null;
                    while (true) {
                        String line;
                        DiskStringReader.ReadEntry tp = reader.readLineAndFileName();
                        if (tp == null) break;
                        line = tp.content;
                        filename = tp.filename;
                        long offset = tp.offset;
                        Map<String, String> attr = Utils.ParseEntryStrToMap(line);
                        for (Map.Entry<String, String> t : attr.entrySet()) {
                            threadAttrTable.put(t.getKey(), Config.BuyerTable);
                        }
                        String buyerid = attr.get("buyerid");
                        long buyerIdHashVal = Utils.hash(buyerid);

                        int buyerBlockId = (int)(buyerIdHashVal % buyerBlockNum);
                        String buyerIndexPath = unSortedBuyerBuyerIndexBlockFiles.get(buyerBlockId);

                        BufferedOutputStream bos = buyerBuyerIndexBlockFilesOutputStreamMapper.get(buyerIndexPath);
                        synchronized (bos) {
                            try {
                                bos.write(Utils.longToBytes(buyerIdHashVal));
                                bos.write(Utils.longToBytes(Utils.ZipFileIdAndOffset(fileIdMapper.get(filename), offset)));
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        ++threadTotal;
                    }
                    total.addAndGet(threadTotal);
                    synchronized (attrToTable) {
                        attrToTable.putAll(threadAttrTable);
                    }
                }
            };
            ths[i].start();
        };
        for (int i = 0; i < threadNumber; ++i) {
            ths[i].join();
        }

        return total.get();
    }

    private long ExtractOrderOffset(List<String> orderFiles) throws IOException, KeyException, InterruptedException {
        final AtomicLong total = new AtomicLong(0L);

        int threadNumber = 16;
        final List<List<String>> threadFiles = Utils.SplitFiles(orderFiles, threadNumber);
        Thread ths[] = new Thread[threadNumber];
        for (int i = 0; i < threadNumber; ++i) {
            final DiskStringReader reader = new DiskStringReader(threadFiles.get(i));
            ths[i] = new Thread() {
                public void run() {
                    String line;
                    String filename = null;
                    long threadTotal = 0;
                    Map<String, String> threadAttrTable = new HashMap<>();
                    while (true) {
                        DiskStringReader.ReadEntry tp = reader.readLineAndFileName();
                        if (tp == null) break;

                        line = tp.content;
                        filename = tp.filename;
                        long offset = tp.offset;
                        Map<String, String> attr = Utils.ParseEntryStrToMap(line);
                        for (Map.Entry<String, String> t : attr.entrySet()) {
                            threadAttrTable.put(t.getKey(), Config.OrderTable);
                        }
                        long orderId = Long.parseLong(attr.get("orderid"));
                        String goodid = attr.get("goodid");
                        String buyerid = attr.get("buyerid");
                        long createtime = Long.parseLong(attr.get("createtime"));


                        int orderBlockId = (int) (orderId % orderBlockNum);
                        String orderIndexPath = unSortedOrderOrderIndexBlockFiles.get(orderBlockId);
                        diskWriterMap.get(Utils.GetDisk(orderIndexPath)).write(orderIndexPath, Utils.longToBytes(orderId, Utils.ZipFileIdAndOffset(fileIdMapper.get(filename), offset)));


                        long goodHashVal = Utils.hash(goodid);
                        int goodBlockId = (int) ((goodHashVal) % orderBlockNum);
                        String goodIndexPath = unSortedOrderGoodIndexBlockFiles.get(goodBlockId);
                        diskWriterMap.get(Utils.GetDisk(goodIndexPath)).write(goodIndexPath, Utils.longToBytes(goodHashVal, Utils.ZipFileIdAndOffset(fileIdMapper.get(filename), offset)));

                        long buyerHashVal = Utils.hash(buyerid);
                        Tuple<Long, Long> buyerIndexEntry = new Tuple<>(buyerHashVal, createtime);
                        int buyerBlockId = buyerBlockMapper.floorEntry(buyerIndexEntry).getValue();
                        String buyerIndexPath = unSortedOrderBuyerIndexBlockFiles.get(buyerBlockId);
                        diskWriterMap.get(Utils.GetDisk(buyerIndexPath)).write(buyerIndexPath, Utils.longToBytes(buyerHashVal, createtime, Utils.ZipFileIdAndOffset(fileIdMapper.get(filename), offset)));

                        ++threadTotal;

                    }
                    total.addAndGet(threadTotal);
                    synchronized (attrToTable) {
                        attrToTable.putAll(threadAttrTable);
                    }
                }
            };
            ths[i].start();

        }
        for (int i = 0; i < threadNumber; ++i) {
            ths[i].join();
        }
        return total.get();
    }
    private Map<String, TreeMap<Long, Long>> SortOffset(final List<String> unOrderedFiles, final List<String> orderedFiles, final long ratio) throws IOException, KeyException, InterruptedException {
        final Map<String, TreeMap<Long, Long>> res = new HashMap<>();
        final int threadNum = 3;
        Thread[] ths = new Thread[threadNum];
        for (int t = 0; t < threadNum; ++t) {
            final int tid = t;
            ths[t] = new Thread(){
                public void run() {
                    try {
                        for (int i = 0; i < unOrderedFiles.size(); ++i) {
                            if (i % threadNum != tid) continue;
                            String unOrderedFilename = unOrderedFiles.get(i);
                            String orderedFilename = orderedFiles.get(i);
                            File file = new File(unOrderedFilename);
                            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file), bufferSize);
                            int entryLength = 16;
                            long offset = 0;
                            //Map<Long, Tuple<Long, Long>> indexMapper = new TreeMap<Long, Tuple<Long, Long>>();
                            List<Tuple<Long, Long>> indexList = new ArrayList<>();
                            while (true) {
                                byte[] entryBytes = new byte[entryLength];
                                int len = bis.read(entryBytes);
                                if (len == -1) break;
                                long[] e = Utils.byteArrayToLongArray(entryBytes);
                                indexList.add(new Tuple<Long, Long>(e[0], e[1]));
                            }
                            bis.close();
                            Collections.sort(indexList, new Comparator<Tuple<Long, Long>>() {
                                @Override
                                public int compare(Tuple<Long, Long> o1, Tuple<Long, Long> o2) {
                                    return o1.x.compareTo(o2.x);
                                }
                            });
                            TreeMap<Long, Long> currentMap = new TreeMap<>();
                            BufferedOutputStream fos = new BufferedOutputStream(new FileOutputStream(orderedFilename), bufferSize);
                            int cnt = 0;
                            for (int idx = 0; idx < indexList.size(); ++idx) {
                                Tuple<Long, Long> e = indexList.get(idx);

                                fos.write(Utils.longToBytes(e.x));
                                fos.write(Utils.longToBytes(e.y));
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
                            synchronized (res) {
                                res.put(orderedFilename, currentMap);
                            }
                            fos.close();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };
            ths[t].start();
        }
        for (int i = 0; i < threadNum; ++i) {
            ths[i].join();
        }
        return res;
    }
    private Map<String, TreeMap<Tuple<Long, Long>, Long>> SortBuyerOffset(final List<String> unOrderedFiles, final List<String> orderedFiles, final long ratio) throws IOException, KeyException, InterruptedException {
        final Map<String, TreeMap<Tuple<Long, Long>, Long>> res = new HashMap<>();
        final int threadNum = 3;
        Thread[] ths = new Thread[threadNum];
        for (int t = 0; t < threadNum; ++t) {
            final int tid = t;
            ths[t] = new Thread(){
                public void run() {
                    try {
                        for (int i = 0; i < unOrderedFiles.size(); ++i) {
                            if (i % threadNum != tid) continue;
                            String unOrderedFilename = unOrderedFiles.get(i);
                            String orderedFilename = orderedFiles.get(i);
                            File file = new File(unOrderedFilename);
                            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file), bufferSize);
                            int entryLength = 24;
                            long offset = 0;
                            //Map<Long, Tuple<Long, Long>> indexMapper = new TreeMap<Long, Tuple<Long, Long>>();
                            List<Tuple<Tuple<Long, Long>, Long>> indexList = new ArrayList<>();
                            while (true) {
                                byte[] entryBytes = new byte[entryLength];
                                int len = bis.read(entryBytes);
                                if (len == -1) break;
                                long[] e = Utils.byteArrayToLongArray(entryBytes);
                                indexList.add(new Tuple<Tuple<Long, Long>, Long>(new Tuple<Long, Long>(e[0], e[1]), e[2]));

                            }
                            bis.close();
                            Collections.sort(indexList);
                            TreeMap<Tuple<Long, Long>, Long> currentMap = new TreeMap<>();
                            BufferedOutputStream fos = new BufferedOutputStream(new FileOutputStream(orderedFilename), bufferSize);
                            int cnt = 0;
                            for (int idx = 0; idx < indexList.size(); ++idx) {
                                Tuple<Tuple<Long, Long>, Long> e = indexList.get(idx);

                                fos.write(Utils.longToBytes(e.x.x));
                                fos.write(Utils.longToBytes(e.x.y));
                                fos.write(Utils.longToBytes(e.y));
                                ++cnt;
                                if (cnt % ratio == 0) {
                                    currentMap.put(e.x, offset);
                                }
                                offset += entryLength;
                            }
                            currentMap.put(indexList.get(0).x, 0L);
                            Tuple<Long, Long> upperBound = new Tuple<>(indexList.get(indexList.size() - 1).x.x, indexList.get(indexList.size() - 1).x.y + 1);
                            currentMap.put(upperBound, offset);
                            synchronized (res) {
                                res.put(orderedFilename, currentMap);
                            }

                            fos.close();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };
            ths[t].start();
        }
        for (int i = 0; i < threadNum; ++i) {
            ths[i].join();
        }
        return res;
    }
    private void SortOffsetParallel() {

        try {
            Thread[] ths = new Thread[disks.size()];
            int cnt = 0;
            for (String s : disks) {
                final String disk = s;
                Thread t = new Thread() {
                    public void run() {
                        try {
                            long orderOrderRatio = orderEntriesCount.get() / memoryOrderOrderIndexSize;
                            if (orderOrderRatio == 0) orderOrderRatio = 1;
                            Map<String, TreeMap<Long, Long>> t1 = SortOffset(Utils.filterByDisk(unSortedOrderOrderIndexBlockFiles, disk), Utils.filterByDisk(sortedOrderOrderIndexBlockFiles, disk), orderOrderRatio);
                            synchronized (orderOrderIndexOffset) {
                                orderOrderIndexOffset.putAll(t1);
                            }

                            long orderGoodRatio = goodEntriesCount.get() / memoryOrderGoodIndexSize;
                            if (orderGoodRatio == 0) orderGoodRatio = 1;
                            Map<String, TreeMap<Long, Long>> t2 = SortOffset(Utils.filterByDisk(unSortedOrderGoodIndexBlockFiles, disk), Utils.filterByDisk(sortedOrderGoodIndexBlockFiles, disk), orderGoodRatio);
                            synchronized (orderGoodIndexOffset) {
                                orderGoodIndexOffset.putAll(t2);
                            }

                            long orderBuyerRatio = orderEntriesCount.get() / memoryOrderBuyerIndexSize;
                            if (orderBuyerRatio == 0) orderBuyerRatio = 1;
                            Map<String, TreeMap<Tuple<Long, Long>, Long>> t3 = SortBuyerOffset(Utils.filterByDisk(unSortedOrderBuyerIndexBlockFiles, disk), Utils.filterByDisk(sortedOrderBuyerIndexBlockFiles, disk), orderBuyerRatio);
                            synchronized (orderBuyerIndexOffset) {
                                orderBuyerIndexOffset.putAll(t3);
                            }

                            long buyerBuyerRatio = buyerEntriesCount.get() / memoryBuyerBuyerIndexSize;
                            if (buyerBuyerRatio == 0) buyerBuyerRatio = 1;
                            Map<String, TreeMap<Long, Long>> t4 = SortOffset(Utils.filterByDisk(unSortedBuyerBuyerIndexBlockFiles, disk), Utils.filterByDisk(sortedBuyerBuyerIndexBlockFiles, disk), buyerBuyerRatio);
                            synchronized (buyerBuyerIndexOffset) {
                                buyerBuyerIndexOffset.putAll(t4);
                            }

                            long goodGoodRatio = orderEntriesCount.get() / memoryGoodGoodIndexSize;
                            if (goodGoodRatio == 0) goodGoodRatio = 1;
                            Map<String, TreeMap<Long, Long>> t5 = SortOffset(Utils.filterByDisk(unSortedGoodGoodIndexBlockFiles, disk), Utils.filterByDisk(sortedGoodGoodIndexBlockFiles, disk), goodGoodRatio);
                            synchronized (goodGoodIndexOffset) {
                                goodGoodIndexOffset.putAll(t5);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                };
                t.start();
                ths[cnt++] = t;
            }
            for (Thread th : ths) {
                th.join();
            }
        } catch (Exception e) {

        }
    }
    private void PreProcessOrders(List<String> orderFiles, List<String> buyerFiles, List<String> goodFiles, List<String> storeFolders) throws IOException, KeyException, InterruptedException {
        //goodRawFileOffset = ExtractGoodOffset(goodFiles);
        //buyerRawFileOffset = ExtractBuyerOffset(buyerFiles);
        long start = System.currentTimeMillis();
        disks = Utils.GetDisks(storeFolders);
        for (String orderFile : orderFiles) {
            fileIdMapperRev.put(fileIdMapperRev.size(), orderFile);
            fileIdMapper.put(orderFile, fileIdMapper.size());
        }
        for (String buyerFile : buyerFiles) {
            fileIdMapperRev.put(fileIdMapperRev.size(), buyerFile);
            fileIdMapper.put(buyerFile, fileIdMapper.size());
        }
        for (String goodFile : goodFiles) {
            fileIdMapperRev.put(fileIdMapperRev.size(), goodFile);
            fileIdMapper.put(goodFile, fileIdMapper.size());
        }

        List<Tuple<Long, Long>> randomBuyerEntries = RandomOrder(orderFiles, orderBlockNum - 1);
        Random rd = new Random(123);
        int diskCount = 0;
        for (int i = 0; i < orderBlockNum; ++i) {
            String currentStoreFolder = storeFolders.get((diskCount++) % storeFolders.size());
            String unSortedOrderPath = currentStoreFolder + "\\oo" + rd.nextInt() + "_";
            String sortedOrderPath = currentStoreFolder + "\\oo" + rd.nextInt();
            unSortedOrderOrderIndexBlockFiles.add(unSortedOrderPath);
            sortedOrderOrderIndexBlockFiles.add(sortedOrderPath);
        }


        for (int i = 0; i < orderBlockNum; ++i) {
            String currentStoreFolder = storeFolders.get((diskCount++) % storeFolders.size());
            String unSortedGoodPath = currentStoreFolder + "\\og" + rd.nextInt() + "_";
            String sortedGoodPath = currentStoreFolder + "\\og" + rd.nextInt();
            unSortedOrderGoodIndexBlockFiles.add(unSortedGoodPath);
            sortedOrderGoodIndexBlockFiles.add(sortedGoodPath);
        }

        buyerBlockMapper.put(new Tuple<Long, Long>(-1L, -1L), 0);
        for (Tuple<Long, Long> e : randomBuyerEntries) {
            buyerBlockMapper.put(e, buyerBlockMapper.size());
        }
        for (int i = 0; i < buyerBlockMapper.size(); ++i) {
            String currentStoreFolder = storeFolders.get((diskCount++) % storeFolders.size());
            String unSortedBuyerPath = currentStoreFolder + "\\ob" + rd.nextInt() + "_";
            String sortedBuyerPath = currentStoreFolder + "\\ob" + rd.nextInt();
            unSortedOrderBuyerIndexBlockFiles.add(unSortedBuyerPath);
            sortedOrderBuyerIndexBlockFiles.add(sortedBuyerPath);
        }

        for (int i = 0; i < buyerBlockNum; ++i) {
            String currentStoreFolder = storeFolders.get((diskCount++) % storeFolders.size());
            String unSortedBuyerPath = currentStoreFolder + "\\bb" + rd.nextInt() + "_";
            String sortedBuyerPath = currentStoreFolder + "\\bb" + rd.nextInt();
            unSortedBuyerBuyerIndexBlockFiles.add(unSortedBuyerPath);
            sortedBuyerBuyerIndexBlockFiles.add(sortedBuyerPath);
            buyerBuyerIndexBlockFilesOutputStreamMapper.put(unSortedBuyerPath, new BufferedOutputStream(new FileOutputStream(unSortedBuyerPath), bufferSize));
        }
        for (int i = 0; i < goodBlockNum; ++i) {
            String currentStoreFolder = storeFolders.get((diskCount++) % storeFolders.size());
            String unSortedGoodPath = currentStoreFolder + "\\gg" + rd.nextInt() + "_";
            String sortedGoodPath = currentStoreFolder + "\\gg" + rd.nextInt();
            unSortedGoodGoodIndexBlockFiles.add(unSortedGoodPath);
            sortedGoodGoodIndexBlockFiles.add(sortedGoodPath);
            goodGoodIndexBlockFilesOutputStreamMapper.put(unSortedGoodPath, new BufferedOutputStream(new FileOutputStream(unSortedGoodPath), bufferSize));
        }
        List<String> allUnsortedOrderIndexFiles = new ArrayList<>();
        allUnsortedOrderIndexFiles.addAll(unSortedOrderOrderIndexBlockFiles);
        allUnsortedOrderIndexFiles.addAll(unSortedOrderGoodIndexBlockFiles);
        allUnsortedOrderIndexFiles.addAll(unSortedOrderBuyerIndexBlockFiles);
        List<List<String>> allUnsortedOrderIndexFilesGroupByDisk = Utils.GroupByDisk(allUnsortedOrderIndexFiles);
        for (List<String> ls : allUnsortedOrderIndexFilesGroupByDisk) {
            String diskTag = Utils.GetDisk(ls.get(0));
            diskWriterMap.put(diskTag, new DiskBytesWriter(ls));
        }

        final List<List<String>> orderFilesGroupByDisk = Utils.GroupByDisk(orderFiles);
        final List<List<String>> goodFilesGroupByDisk = Utils.GroupByDisk(goodFiles);
        final List<List<String>> buyerFilesGroupByDisk = Utils.GroupByDisk(buyerFiles);
        
        Thread[] t1 = new Thread[orderFilesGroupByDisk.size()];
        for (int i = 0; i < t1.length; ++i) {
            final int v = i;
            Thread t = new Thread() {
                public void run() {
                    try {
                        long cnt = ExtractOrderOffset(orderFilesGroupByDisk.get(v));
                        orderEntriesCount.addAndGet(cnt);
                        cnt = ExtractGoodOffset(goodFilesGroupByDisk.get(v));
                        goodEntriesCount.addAndGet(cnt);
                        cnt = ExtractBuyerOffset(buyerFilesGroupByDisk.get(v));
                        buyerEntriesCount.addAndGet(cnt);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };
            t.start();
            t1[i] = t;
        }
        for (int i = 0; i < t1.length; ++i) {
            t1[i].join();
        }

        for (DiskBytesWriter dbw : diskWriterMap.values()) {
            dbw.close();
        }

        for (BufferedOutputStream s : buyerBuyerIndexBlockFilesOutputStreamMapper.values()) {
            s.close();
        }
        for (BufferedOutputStream s : goodGoodIndexBlockFilesOutputStreamMapper.values()) {
            s.close();
        }
        System.out.printf("Classify complete, Time: %d\n", (System.currentTimeMillis() - start) / 1000);
        start = System.currentTimeMillis();
        SortOffsetParallel();
        System.out.printf("Sort complete, Time: %d\n", (System.currentTimeMillis() - start) / 1000);
        for (String path : sortedOrderOrderIndexBlockFiles) {
            BigMappedByteBuffer buf = new BigMappedByteBuffer(path, Integer.MAX_VALUE);
            mbbMap.put(path, buf);
        }
        for (String path : sortedOrderBuyerIndexBlockFiles) {
            BigMappedByteBuffer buf = new BigMappedByteBuffer(path, Integer.MAX_VALUE);
            mbbMap.put(path, buf);
        }
        for (String path : sortedOrderGoodIndexBlockFiles) {
            BigMappedByteBuffer buf = new BigMappedByteBuffer(path, Integer.MAX_VALUE);
            mbbMap.put(path, buf);
        }
        for (String path : sortedBuyerBuyerIndexBlockFiles) {
            BigMappedByteBuffer buf = new BigMappedByteBuffer(path, Integer.MAX_VALUE);
            mbbMap.put(path, buf);
        }
        for (String path : sortedGoodGoodIndexBlockFiles) {
            BigMappedByteBuffer buf = new BigMappedByteBuffer(path, Integer.MAX_VALUE);
            mbbMap.put(path, buf);
        }
        for (String path : fileIdMapper.keySet()) {
            BigMappedByteBuffer buf = new BigMappedByteBuffer(path, Integer.MAX_VALUE);
            mbbMap.put(path, buf);
        }
    }
    private List<String> QueryEntryById(long id, long blockNum, Map<String, TreeMap<Long, Long>> indexOffset, List<String> sortedIndexBlockFiles, Map<Integer, String> fileIdMapperRev) {
        int blockId = (int)(id % blockNum);
        TreeMap<Long, Long> blockIndex = indexOffset.get(sortedIndexBlockFiles.get(blockId));
        long offset = blockIndex.floorEntry(id).getValue();
        int len = (int)(blockIndex.higherEntry(id).getValue() - offset);

        try {
            byte[] buf = new byte[len];
            //FileChannel fc = FileChannel.open(Paths.get(sortedIndexBlockFiles.get(blockId)));
            BigMappedByteBuffer fc = mbbMap.get(sortedIndexBlockFiles.get(blockId)).slice();
            fc.position((int)offset);
            fc.get(buf);

            long[] ls = Utils.byteArrayToLongArray(buf);
            List<Tuple<Long, Long>> r = new ArrayList<>();
            List<String> ans = new ArrayList<>();
            for (int i = 0; i < ls.length; i += 2) {
                if (ls[i] == id) {
                    long cacheKey = ls[i + 1];
                    Tuple<Long, Long> tp = Utils.UnZipFileIdAndOffset(cacheKey);
                    long fileId = tp.x;
                    long rawOffset = tp.y;
                    String rawFilename = fileIdMapperRev.get((int) fileId);
                    String line = rawDataCache.get(cacheKey);
                    if (line == null) {
                        BigMappedByteBuffer rfc = mbbMap.get(rawFilename).slice();

                        BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteBufferBackedInputStream(rfc, rawOffset), "UTF-8"), 64);
                        line = reader.readLine();
                        rawDataCache.put(cacheKey, line);
                    }
                    ans.add(line);
                }
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
        //int blockId = buyerBlockMapper.floorEntry(buyerIndexEntryLowerBound).getValue();
        List<String> ans = new ArrayList<>();

        Tuple<Long, Long> buyerIndexSubmapLowerBound = buyerBlockMapper.floorKey(buyerIndexEntryLowerBound);
        for (int blockId : buyerBlockMapper.subMap(buyerIndexSubmapLowerBound, buyerIndexEntryUpperBound).values()) {

            TreeMap<Tuple<Long, Long>, Long> blockIndex = indexOffset.get(sortedIndexBlockFiles.get(blockId));
            Map.Entry<Tuple<Long, Long>, Long> floorEntry = blockIndex.floorEntry(buyerIndexEntryLowerBound);
            Map.Entry<Tuple<Long, Long>, Long> ceilingEntry = blockIndex.ceilingEntry(buyerIndexEntryUpperBound);
            if (floorEntry == null ) {
                floorEntry = blockIndex.firstEntry();
            }
            if (ceilingEntry == null) {
                ceilingEntry = blockIndex.lastEntry();
            }
            long offset = floorEntry.getValue();
            int len = (int) (ceilingEntry.getValue() - offset);

            try {
                //File file = new File(sortedIndexBlockFiles.get(blockId));
                byte[] buf = new byte[len];
                BigMappedByteBuffer fc = mbbMap.get(sortedIndexBlockFiles.get(blockId)).slice();

                fc.position((int)offset);
                fc.get(buf);

                long[] ls = Utils.byteArrayToLongArray(buf);
                List<Tuple<Long, Long>> r = new ArrayList<>();
                for (int i = 0; i < ls.length; i += 3) {
                    if (ls[i] == buyerHashVal && ls[i + 1] >= from && ls[i + 1] < to) {
                        Tuple<Long, Long> tp = Utils.UnZipFileIdAndOffset(ls[i + 2]);
                        long fileId = tp.x;
                        long rawOffset = tp.y;
                        r.add(new Tuple<Long, Long>(fileId, rawOffset));
                    }
                }

                for (Tuple<Long, Long> item : r) {
                    long fileId = item.x;
                    long rawOffset = item.y;

                    String rawFilename = fileIdMapperRev.get((int) fileId);
                    String line = null;
                    if (line == null) {
                        BigMappedByteBuffer rfc = mbbMap.get(rawFilename).slice();

                        BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteBufferBackedInputStream(rfc, rawOffset), "UTF-8"), 64);
                        line = reader.readLine();

                    }
                    ans.add(line);

                }

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
            }
        }
        return ans;
    }

    private String QueryBuyerByBuyer(String buyerid) {
        List<String> ans = QueryEntryById(Utils.hash(buyerid), buyerBlockNum, buyerBuyerIndexOffset, sortedBuyerBuyerIndexBlockFiles, fileIdMapperRev);
        return ans.get(0);
    }

    private String QueryGoodByGood(String goodid) {
        List<String> ans = QueryEntryById(Utils.hash(goodid), goodBlockNum, goodGoodIndexOffset, sortedGoodGoodIndexBlockFiles, fileIdMapperRev);
        return ans.get(0);
    }

    public void constructThread(Collection<String> orderFiles, Collection<String> buyerFiles, Collection<String> goodFiles, Collection<String> storeFolders) {
        try {
            long start = System.currentTimeMillis();
            PreProcessOrders(new ArrayList<String>(orderFiles), new ArrayList<String>(buyerFiles), new ArrayList<String>(goodFiles), new ArrayList<String>(storeFolders));
            System.out.printf("Construct complete, order: %d, good: %d, buyer: %d\n", orderEntriesCount.get(), goodEntriesCount.get(), buyerEntriesCount.get());
            for (Map.Entry<String, BigMappedByteBuffer> e : mbbMap.entrySet()) {
                System.out.printf("File name: %s, size: %d\n", e.getKey(), e.getValue().remaining());
            }
            System.out.printf("total attributes: %d\n", attrToTable.size());
            for (Map.Entry<String, String> e : attrToTable.entrySet()) {
                System.out.println(e.getKey() + "\t" + e.getValue());
            }
            System.out.printf("total construct time: %dms\n", System.currentTimeMillis() - start);
            synchronized (constructFinishNotifier) {
                constructFinish = true;
                constructFinishNotifier.notifyAll();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Override
    public void construct(final Collection<String> orderFiles, final Collection<String> buyerFiles, final Collection<String> goodFiles, final Collection<String> storeFolders) {
        Thread t = new Thread() {
            public void run() {
                constructThread(orderFiles, buyerFiles, goodFiles, storeFolders);
            }
        };
        t.start();
        synchronized (constructFinishNotifier) {
            while (!constructFinish) {
                try {
                    constructFinishNotifier.wait(3550 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    @Override
    public Result queryOrder(long orderId, Collection<String> keys) {
        synchronized (constructFinishNotifier) {
            while (!constructFinish) {
                try {
                    constructFinishNotifier.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        List<String> ans = QueryEntryById(orderId, orderBlockNum, orderOrderIndexOffset, sortedOrderOrderIndexBlockFiles, fileIdMapperRev);
        Set<String> attrs = null;
        if (keys == null) {
            keys = attrToTable.keySet();
        }
        attrs = new HashSet<>(keys);
        if (ans.isEmpty()) return null;
        String r = ans.get(0);
        Map<String, String> orderLs = Utils.ParseEntryStrToMap(r);

        for (String key : keys) {
            if (Config.BuyerTable.equals(attrToTable.get(key)) && !key.equals("buyerid")) {
                String buyerStr = QueryBuyerByBuyer(orderLs.get("buyerid"));
                orderLs.putAll(Utils.ParseEntryStrToMap(buyerStr));
                break;
            }
        }

        for (String key : keys) {
            if (Config.GoodTable.equals(attrToTable.get(key)) && !key.equals("goodid")) {
                String goodStr = QueryGoodByGood(orderLs.get("goodid"));
                orderLs.putAll(Utils.ParseEntryStrToMap(goodStr));
            }
        }

        HashMap<String, String> rt = new HashMap<>();
        for (Map.Entry<String, String> t : orderLs.entrySet()) {
            if (t.getKey().equals("orderid") || attrs.contains(t.getKey())) {
                rt.put(t.getKey(), t.getValue());
            }
        }

        System.out.print("Ord,k:");
        if (keys.size() == attrToTable.size()) {
            System.out.print("null");
        } else {
            for (String k : keys) {
                System.out.print(k + ",");
            }
        }
        System.out.println();
        return new QueryResult(rt);

    }

    @Override
    public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime, String buyerid) {
        synchronized (constructFinishNotifier) {
            while (!constructFinish) {
                try {
                    constructFinishNotifier.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        List<String> ans = QueryOrderByBuyer(Utils.hash(buyerid), startTime, endTime, orderBuyerIndexOffset, sortedOrderBuyerIndexBlockFiles);
        List<Result> rr = new ArrayList<>();
        if (ans.isEmpty()) return rr.iterator();
        Map<String, String> buyerInfo = Utils.ParseEntryStrToMap(QueryBuyerByBuyer(buyerid));

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
        System.out.printf("Buy,l:%d\n", rr.size());
        return rr.iterator();

    }

    @Override
    public Iterator<Result> queryOrdersBySaler(String salerid, String goodid, Collection<String> keys) {
        synchronized (constructFinishNotifier) {
            while (!constructFinish) {
                try {
                    constructFinishNotifier.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        List<String> ans = QueryEntryById(Utils.hash(goodid), orderBlockNum, orderGoodIndexOffset, sortedOrderGoodIndexBlockFiles, fileIdMapperRev);
        Set<String> attrs = null;
        if (keys == null) {
            keys = attrToTable.keySet();
        }
        attrs = new HashSet<>(keys);
        attrs.add("orderid");
        List<Result> rr = new ArrayList<>();
        if (ans.isEmpty()) return rr.iterator();

        Map<String, String> goodAttr = new HashMap<>();
        boolean loadGoodTable = false;
        boolean loadBuyerTable = false;
        for (String key : keys) {
            if (Config.GoodTable.equals(attrToTable.get(key)) && !key.equals("goodid")) {
                loadGoodTable = true;
            }
            if (Config.BuyerTable.equals(attrToTable.get(key)) && !key.equals("buyerid")) {
                loadBuyerTable = true;
            }
        }
        if (loadGoodTable) {
            String goodStr = QueryGoodByGood(goodid);
            Map<String, String> t = Utils.ParseEntryStrToMap(goodStr);
            for (Map.Entry<String, String> e : t.entrySet()) {
                if (attrs.contains(e.getKey())) {
                    goodAttr.put(e.getKey(), e.getValue());
                }
            }
        }
        if (loadGoodTable) {
            attrs.add("goodid");
        }
        if (loadBuyerTable) {
            attrs.add("buyerid");
        }
        for (String r : ans) {
            Map<String, String> orderLs = new HashMap<>();
            for (Map.Entry<String, String> e : Utils.ParseEntryStrToMap(r).entrySet()) {
                if (attrs.contains(e.getKey())) {
                    orderLs.put(e.getKey(), e.getValue());
                }
            }
            if (loadBuyerTable) {
                String buyerStr = QueryBuyerByBuyer(orderLs.get("buyerid"));
                for (Map.Entry<String, String> e : Utils.ParseEntryStrToMap(buyerStr).entrySet()) {
                    if (attrs.contains(e.getKey())) {
                        orderLs.put(e.getKey(), e.getValue());
                    }
                }
            }
            orderLs.putAll(goodAttr);

            HashMap<String, String> rt = new HashMap<>();
            for (Map.Entry<String, String> t : orderLs.entrySet()) {
                rt.put(t.getKey(), t.getValue());
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
        System.out.printf("Good,l:%d,k:", rr.size());
        if (keys.size() == attrToTable.size()) {
            System.out.print("null");
        } else {
            for (String k : keys) {
                System.out.print(k + ",");
            }
        }
        System.out.println();
        return rr.iterator();

    }

    @Override
    public KeyValue sumOrdersByGood(String goodid, String key) {
        synchronized (constructFinishNotifier) {
            while (!constructFinish) {
                try {
                    constructFinishNotifier.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        //List<String> ans = QueryEntryById(Utils.hash(goodid), orderBlockNum, orderGoodIndexOffset, sortedOrderGoodIndexBlockFiles, orderFileIdMapperRev);
        Iterator<Result> ans = queryOrdersBySaler("", goodid, Arrays.asList(key));
        if (!ans.hasNext()) return null;
        long longSum = 0L;
        double doubleSum = 0.0;
        boolean isDouble = false;
        int cnt = 0;
        try {
            while (ans.hasNext()) {
                Result pr = ans.next();
                String t = pr.get(key).valueAsString();
                if (t == null) {
                    continue;
                }
                ++cnt;
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
            /*
            longSum = 0;
            doubleSum = 0.0;
            */
            return null;
        }
        if (cnt == 0) return null;
        QueryKeyValue kv = new QueryKeyValue(key, isDouble ? ((Double) doubleSum).toString() : ((Long) longSum).toString());
        return kv;

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
