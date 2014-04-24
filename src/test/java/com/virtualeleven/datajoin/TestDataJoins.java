package com.virtualeleven.datajoin;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.xml.DOMConfigurator;
import org.junit.Before;
import org.junit.Test;

public class TestDataJoins {

    /**
     * Orders
     *
     * 3,A,12.95,02-Jun-2008
     * 1,B,88.25,20-May-2008
     * 2,C,32.00,30-Nov-2007
     * 3,D,25.02,22-Jan-2009
     *
     * Customers
     *
     * 1,Stephanie Leung,555-555-5555
     * 2,Edward Kim,123-456-7890
     * 3,Jose Madriz,281-330-8004
     * 4,David Stork,408-555-0000
     */
    private final File customersInputFile = new File("src/test/resources/Customers.txt");
    private final File ordersInputFile = new File("src/test/resources/Orders.txt");
    private final File output = new File("./target/test-result/");

    @Before
    public void setUpTest() throws IOException {
        FileUtils.deleteQuietly(output);
    }

    @Test
    public void testRun() throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.default.name", "file:///");
        DOMConfigurator.configure("src/main/resources/log4j.xml");
        DataJoinJob underTest = new DataJoinJob();

        underTest.setConf(conf);
        int exitCode = underTest.run(new String[] {
                customersInputFile.getAbsolutePath(),
                ordersInputFile.getAbsolutePath(), output.getAbsolutePath() });
        Assert.assertEquals("Returned error code.", 0, exitCode);
        Assert.assertTrue(new File(output, "_SUCCESS").exists());
        Set<String> result = getResultAsMap(new File(output, "part-00000"));

        Assert.assertTrue(result
                .contains("1,Stephanie Leung,555-555-5555,B,88.25,20-May-2008"));
        Assert.assertTrue(result
                .contains("2,Edward Kim,123-456-7890,C,32.00,30-Nov-2007"));
        Assert.assertTrue(result
                .contains("3,Jose Madriz,281-330-8004,D,25.02,22-Jan-2009"));
        Assert.assertTrue(result
                .contains("3,Jose Madriz,281-330-8004,A,12.95,02-Jun-2008"));
    }

	private Set<String> getResultAsMap(File file) throws IOException {
		Set<String> result = new HashSet<String>();
		String contentOfFile = FileUtils.readFileToString(file);
		for (String line : contentOfFile.split("\n")) {
			result.add(line);
		}
		return result;
	}

}
