package com.alibaba.middleware.race;

import com.alibaba.middleware.race.diskio.DiskBytesWriter;
import com.alibaba.middleware.race.diskio.DiskStringReader;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by hahong on 2016/7/24.
 */
public class ExternalSort {
    public ExternalSort(Collection<String> orderFiles, List<String> buyerBlockFiles, List<String> goodBlockFiles, Map<String, DiskBytesWriter> diskWriters) {
        DiskStringReader dsr = new DiskStringReader(orderFiles);
        String line;
        int goodBlockNum = goodBlockFiles.size();
        while ((line = dsr.readLine()) != null) {
            Map<String, String> attrs = Utils.ParseEntryStrToMap(line);
            String goodId = attrs.get("goodid");
            int goodFileId = (int)(Utils.hash(goodId) % goodBlockNum);

            String goodBlockFile = goodBlockFiles.get(goodFileId);
            String diskTag = Utils.GetDisk(goodBlockFile);
            diskWriters.get(diskTag).write(goodBlockFile, (line + "\n").getBytes(StandardCharsets.UTF_8));
        }
    }

}
