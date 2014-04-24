package com.virtualeleven.datajoin;

import java.io.File;

import org.apache.hadoop.contrib.utils.join.DataJoinMapperBase;
import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
import org.apache.hadoop.io.Text;

public class DataJoinMap extends DataJoinMapperBase {

    @Override
    protected Text generateInputTag(String inputFile) {
        String datasource = new File(inputFile).getName();
        return new Text(datasource);
    }

    @Override
    protected Text generateGroupKey(TaggedMapOutput aRecord) {
        String line = ((Text) aRecord.getData()).toString();
        String[] tokens = line.split(",");
        String groupKey = tokens[0];
        return new Text(groupKey);
    }

    @Override
    protected TaggedMapOutput generateTaggedMapOutput(Object value) {
        TaggedWritable retv = new TaggedWritable((Text) value);
        retv.setTag(this.inputTag);
        return retv;
    }
}