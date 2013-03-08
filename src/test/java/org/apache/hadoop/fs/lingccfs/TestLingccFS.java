/**
 * Test LingccFS interface using only default Hadoop FileSystem Interface.
 */
package org.apache.hadoop.fs.lingccfs;

import java.net.URI;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.lingccfs.LingccFileSystem;
/**
 * @author lingkun
 *
 */
public class TestLingccFS extends TestCase {
	public static final Log LOG = LogFactory.getLog(TestLingccFS.class);
	LingccFileSystem lingccfs;
	Path baseDir;



	/**
	 * @param name
	 */
	public TestLingccFS(String name) {
		super(name);
	}

	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception {
		LOG.info("Test setup");
		super.setUp();
		Configuration conf = new Configuration();
		lingccfs = new LingccFileSystem();
		lingccfs.initialize(URI.create("lingccfs:///"),conf);
		baseDir = new Path(System.getProperty("test.build.data", "/tmp")
				+"/lingccfs-test");
		LOG.info("BaseDir="+baseDir);
	}

	/* (non-Javadoc)
	 * @see junit.framework.TestCase#tearDown()
	 */
	protected void tearDown() throws Exception {
		super.tearDown();
	}

	// @Test
	// Check all the directory API's in LingccFS
	public void testDirs() throws Exception {
		LOG.info("Run testDirs");
		Path subDir1 = new Path("dir.1");

		// make the dir
		lingccfs.mkdirs(baseDir);
		assertTrue(lingccfs.isDirectory(baseDir));
		lingccfs.setWorkingDirectory(baseDir);

		lingccfs.mkdirs(subDir1);
		assertTrue(lingccfs.isDirectory(subDir1));

		assertFalse(lingccfs.exists(new Path("test1")));
		assertFalse(lingccfs.isDirectory(new Path("test/dir.2")));

		FileStatus[] p = lingccfs.listStatus(baseDir);
		assertEquals(p.length, 1);

		lingccfs.delete(baseDir, true);
		assertFalse(lingccfs.exists(baseDir));
	}

	// @Test
	// Check the file APIs
	public void testFiles() throws Exception {
		LOG.info("Run TestFiles");
		Path subDir1 = new Path("dir.1");
		Path file1 = new Path("dir.1/foo.1");
		Path file2 = new Path("dir.1/foo.2");

		lingccfs.mkdirs(baseDir);
		assertTrue(lingccfs.isDirectory(baseDir));
		lingccfs.setWorkingDirectory(baseDir);

		lingccfs.mkdirs(subDir1);

		FSDataOutputStream s1 = lingccfs.create(file1, true, 4096, (short) 1, (long) 4096, null);
		FSDataOutputStream s2 = lingccfs.create(file2, true, 4096, (short) 1, (long) 4096, null);

		s1.close();
		s2.close();

		FileStatus[] p = lingccfs.listStatus(subDir1);
		assertEquals(p.length, 2);

		lingccfs.delete(file1, true);
		p = lingccfs.listStatus(subDir1);
		assertEquals(p.length, 1);

		lingccfs.delete(file2, true);
		p = lingccfs.listStatus(subDir1);
		assertEquals(p.length, 0);

		lingccfs.delete(baseDir, true);
		assertFalse(lingccfs.exists(baseDir));
	}

	// @Test
	// Check file/read write
	public void testFileIO() throws Exception {
		LOG.info("Run TestFileIO");
		Path subDir1 = new Path("dir.1");
		Path file1 = new Path("dir.1/foo.1");

		lingccfs.mkdirs(baseDir);
		assertTrue(lingccfs.isDirectory(baseDir));
		lingccfs.setWorkingDirectory(baseDir);

		lingccfs.mkdirs(subDir1);

		FSDataOutputStream s1 = lingccfs.create(file1, true, 4096, (short) 1, (long) 4096, null);

		int bufsz = 4096;
		byte[] data = new byte[bufsz];

		for (int i = 0; i < data.length; i++)
			data[i] = (byte) (i % 16);

		// write 4 bytes and read them back; read API should return a byte per call
		s1.write(32);
		s1.write(32);
		s1.write(32);
		s1.write(32);
		// write some data
		s1.write(data, 0, data.length);
		// flush out the changes
		s1.close();

		// Read the stuff back and verify it is correct
		FSDataInputStream s2 = lingccfs.open(file1, 4096);
		int v;

		v = s2.read();
		assertEquals(v, 32);
		v = s2.read();
		assertEquals(v, 32);
		v = s2.read();
		assertEquals(v, 32);
		v = s2.read();
		assertEquals(v, 32);

		assertEquals(s2.available(), data.length);

		byte[] buf = new byte[bufsz];
		s2.read(buf, 0, buf.length);
		for (int i = 0; i < data.length; i++)
			assertEquals(data[i], buf[i]);

		assertEquals(s2.available(), 0);

		s2.close();

		lingccfs.delete(file1, true);
		assertFalse(lingccfs.exists(file1));        
		lingccfs.delete(subDir1, true);
		assertFalse(lingccfs.exists(subDir1));        
		lingccfs.delete(baseDir, true);
		assertFalse(lingccfs.exists(baseDir));        
	}
	
}
