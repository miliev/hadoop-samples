package com.virtualeleven.datajoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TaggedWritable extends TaggedMapOutput {

    private Writable data;

    public TaggedWritable() {
        this(new Text(""));
    }

    public TaggedWritable(Writable data) {
        this.tag = new Text("");
        this.data = data;
    }

    @Override
    public Writable getData() {
        return data;
    }

    public void setData(Writable data) {
        this.data = data;
    }

    public void write(DataOutput out) throws IOException {
        this.tag.write(out);
        this.data.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.tag.readFields(in);
        this.data.readFields(in);
    }
}