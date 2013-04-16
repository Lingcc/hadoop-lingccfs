package org.apache.hadoop.fs.lingccfs;

import java.io.BufferedOutputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;

// TODO: Copy file from local to lingccfs seems have problems.
// TODO: The Permission check framework can be relax currently.

public class LingccFileSystem extends FileSystem {

	private URI uri;
	private URI LingccFSWriteRoot;
	private URI HBaseRootDir;
	private Path workingDir;
	public static final Log LOG = LogFactory.getLog(LingccFileSystem.class);


	public LingccFileSystem () {
		//workingDir = new  Path(System.getProperty("user.dir")).makeQualified(this);

	}

	@Override
	public URI getUri() {
		return uri;
	}
	
	public Path getHomeDirectory() {
		Path Home = makeQualified(new Path("/user/"+ System.getProperty("user.name")));
		LOG.debug("Home Path=" + Home.toString());
		return  Home;
	}

	public void initialize(URI uri, Configuration conf) throws IOException {
		super.initialize(uri, conf);
		setConf(conf);
		
	    //LOG.debug(StringUtils.stringifyException(new IOException("LingccFS Initialize")));

		// lingkun: Do we need to use the fs.default.name for uri?
	    if(conf.get("hbase.rootdir") != null) {
	    	this.HBaseRootDir = URI.create(conf.get("hbase.rootdir"));
	    } else {
	    	this.HBaseRootDir = null;
	    }
	    
	    if(conf.get("fs.default.name") != null) {
	    	this.LingccFSWriteRoot = URI.create(conf.get("fs.default.name"));
	    } else {
	    	this.LingccFSWriteRoot = null;
	    }
	    
	    if(this.HBaseRootDir == null && this.LingccFSWriteRoot == null) {
	    	this.LingccFSWriteRoot = URI.create("lingccfs:///");
	        LOG.warn("Init LingccFS without specifying Hbase root dir or LingccFS default uri"
	        		+ "using the default root path: lingccfs:///");
	    }
	    
		this.uri = URI.create(uri.getScheme()+":///");
		this.workingDir = getHomeDirectory();
		LOG.info("LingccFS Initialized, using LingccFS as the distributed file system");
		LOG.debug("LingccFS initialized Done, uri=" + getUri() 
				+ ",workingDir="+ getWorkingDirectory()
				+ ", LingccFSWriteRoot=" +  LingccFSWriteRoot
				+ ", HBaseRootDir = " + HBaseRootDir);
	}

	static String execCommand(File f, String... cmd) throws IOException {
		String[] args = new String[cmd.length + 1];
		System.arraycopy(cmd, 0, args, 0, cmd.length);
		args[cmd.length] = f.getCanonicalPath();
		String output = Shell.execCommand(args);
		return output;
	}
	
	class TrackingFileInputStream extends FileInputStream {
		public TrackingFileInputStream(File f) throws IOException {
			super(f);
		}

		public int read() throws IOException {
			int result = super.read();
			if (result != -1) {
				statistics.incrementBytesRead(1);
			}
			return result;
		}

		public int read(byte[] data) throws IOException {
			int result = super.read(data);
			if (result != -1) {
				statistics.incrementBytesRead(result);
			}
			return result;
		}

		public int read(byte[] data, int offset, int length) throws IOException {
			int result = super.read(data, offset, length);
			if (result != -1) {
				statistics.incrementBytesRead(result);
			}
			return result;
		}
	}

	/*******************************************************
	 * For open()'s FSInputStream
	 *******************************************************/
	class LingccFSFileInputStream extends FSInputStream {
		FileInputStream fis;
		private long position;

		public LingccFSFileInputStream(Path f) throws IOException {
			LOG.debug("Init LingccFSFileInputStream, Path:"+f.toString());
			this.fis = new TrackingFileInputStream(pathToFile(f));
		}

		public void seek(long pos) throws IOException {
			LOG.debug("LingccFSFileInputStream seek.Position="+pos);
			fis.getChannel().position(pos);
			this.position = pos;
		}

		public long getPos() throws IOException {
			return this.position;
		}

		public boolean seekToNewSource(long targetPos) throws IOException {
			return false;
		}

		/*
		 * Just forward to the fis
		 */
		public int available() throws IOException { return fis.available(); }
		public void close() throws IOException { fis.close(); }
		public boolean markSupport() { return false; }

		public int read() throws IOException {
			LOG.debug("LingccFSFileInputStream read. Line 144");
			try {
				int value = fis.read();
				if (value >= 0) {
					this.position++;
				}
				return value;
			} catch (IOException e) {                 // unexpected exception
				throw new Error(e);                   // assume native fs error
			}
		}

		public int read(byte[] b, int off, int len) throws IOException {
			try {
				int value = fis.read(b, off, len);
				if (value > 0) {
					this.position += value;
				}
				return value;
			} catch (IOException e) {                 // unexpected exception
				throw new Error(e);                   // assume native fs error
			}
		}

		public int read(long position, byte[] b, int off, int len)
				throws IOException {
			ByteBuffer bb = ByteBuffer.wrap(b, off, len);
			try {
				return fis.getChannel().read(bb, position);
			} catch (IOException e) {
				throw new Error(e);
			}
		}

		public long skip(long n) throws IOException {
			long value = fis.skip(n);
			LOG.debug("LingccFSFileInputStream skip. value=" + value);
			if (value > 0) {
				this.position += value;
			}
			return value;
		}
	}




	@Override
	public FSDataInputStream open(Path f, int bufferSize) throws IOException {
		if(!exists(f)) {
			throw new  FileNotFoundException(f.toString());
		}
		LOG.debug("FSDataInputStream Open. Path:"+f.toString()
				+". buffersize="+bufferSize);

		LOG.debug("LingccFS Open. Path:"+f.toString());
		return new FSDataInputStream(new BufferedFSInputStream(
				new LingccFSFileInputStream(f), bufferSize));
	}



	/*********************************************************
	 * For create()'s FSOutputStream.
	 *********************************************************/
	class LingccFSFileOutputStream extends OutputStream implements Syncable {
		FileOutputStream fos;

		private LingccFSFileOutputStream(Path f, boolean append) throws IOException {
			LOG.debug("Init LingccFSFileOutputStream, Path:"+f.toString());

			this.fos = new FileOutputStream(pathToFile(f), append);
		}


		/*
		 * Just forward to the fos
		 */
		public void close() throws IOException { fos.close(); }
		public void flush() throws IOException { fos.flush(); }
		public void write(byte[] b, int off, int len) throws IOException {
			try {
				fos.write(b, off, len);
			} catch (IOException e) {                // unexpected exception
				throw new Error(e);                  // assume native fs error
			}
		}

		public void write(int b) throws IOException {
			LOG.debug("LingccFSFileOutputStream write. b="+ b);
			try {
				fos.write(b);
			} catch (IOException e) {              // unexpected exception
				throw new Error(e);                // assume native fs error
			}
		}

		/** {@inheritDoc} */
		public void sync() throws IOException {
			fos.getFD().sync();      
		}

	}

	public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize,
			short replication, long blockSize, Progressable progress)
					throws IOException {
		LOG.debug("FSDataOutputStream create. NewFile="+f.toString());

		return create(f, overwrite, true, bufferSize, replication, blockSize, progress);
	}

	
	/**
	 * Implement createNonRecursive for HBase HLog Writer 
	 * {@inheritDoc} */
	@Override
	public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
	    boolean overwrite, int bufferSize, short replication, long blockSize,
	    Progressable progress) throws IOException {
			FSDataOutputStream out = create(f,
					overwrite, false, bufferSize, replication, blockSize, progress);
			setPermission(f, permission);
			return out;
	  }
	
	
	/**
	 * Creates the specified directory hierarchy. Does not
	 * treat existence as an error.
	 */
	public boolean mkdirs(Path f) throws IOException {
		
		LOG.debug("LingccFS mkdir. Dir Path="+f.toString());
		boolean res = false;
		
		try {
		Path parent = makeAbsolute(f).getParent();
		File p2f = pathToFile(f);
		res = (parent == null 
				|| (exists(parent) && getFileStatus(parent).isDir())
				||  mkdirs(parent))
				&& ( p2f.isDirectory() || p2f.mkdir() 
			  );
		} catch (IOException e) {
			throw new Error(e);
		}
		return res;
	}

	public boolean mkdirs(Path f, FsPermission permission) throws IOException {
		LOG.debug("LingccFS make dir, Path="+f);
		boolean b = mkdirs(f);
		setPermission(f, permission);
		return b;
	}

	private FSDataOutputStream create(Path fName, boolean overwrite, 
			boolean createParent, int bufferSize,
			short replication, long blockSize, Progressable progress)
					throws IOException {
		
		LOG.debug("LingccFS FSDataOutputStream Create file. Path="+fName.toString());
		
		Path f = makeAbsolute(fName);

		

		if (exists(f) && !overwrite) {
			throw new IOException("File already exists:"+f);
		}
		Path parent = f.getParent();
		if (parent != null) {
			if (!createParent && !exists(parent)) {
				throw new FileNotFoundException("Parent directory doesn't exist: "
						+ parent);
			} else if (!mkdirs(parent)) {
				throw new IOException("Mkdirs failed to create " + parent);
			}
		}
		return new FSDataOutputStream(new BufferedOutputStream(
				new LingccFSFileOutputStream(f, false), bufferSize), statistics);
	}

	public FSDataOutputStream create(Path f, FsPermission permission,
			boolean overwrite, int bufferSize, short replication,
			long blockSize, Progressable progress) throws IOException {
		FSDataOutputStream out = create(f,
				overwrite, bufferSize, replication, blockSize, progress);
		setPermission(f, permission);
		return out;
	}

	public FSDataOutputStream append(Path f, int bufferSize,
			Progressable progress) throws IOException {
		if (!exists(f)) {
			throw new FileNotFoundException("File " + f + " not found.");
		}
		if (getFileStatus(f).isDir()) {
			throw new IOException("Cannot append to a diretory (=" + f + " ).");
		}

		LOG.debug("LingccFS FSDataOutputStream Append file. Path="+f.toString());

		return new FSDataOutputStream(new BufferedOutputStream(
				new LingccFSFileOutputStream(f, true), bufferSize), statistics);
	}

	public boolean rename(Path src, Path dst) throws IOException {
		if (pathToFile(src).renameTo(pathToFile(dst))) {
			return true;
		}
		LOG.debug("LingccFS Rename Falling through to a copy of " + src + " to " + dst);
		return FileUtil.copy(this, src, this, dst, true, getConf());
	}


	public boolean delete(Path f) throws IOException {
		return delete(f, true);
	}

	public boolean delete(Path p, boolean recursive) throws IOException {

		LOG.debug("LingccFS delete" + p);

		File f = pathToFile(p);
		if (f.isFile()) {
			return f.delete();
		} else if ((!recursive) && f.isDirectory() && 
				(FileUtil.listFiles(f).length != 0)) {
			throw new IOException("Directory " + f.toString() + " is not empty");
		}
		return FileUtil.fullyDelete(f);
	}


	public FileStatus[] listStatus(Path f) throws IOException {
		File localf = pathToFile(f);
		FileStatus[] results;

		if (!localf.exists()) {
			return null;
		}
		if (localf.isFile()) {
			return new FileStatus[] {
					new RawLingccFileStatus(localf, getDefaultBlockSize(), this) };
		}

		String[] names = localf.list();
		if (names == null) {
			return null;
		}
		results = new FileStatus[names.length];
		for (int i = 0; i < names.length; i++) {
			results[i] = getFileStatus(new Path(f, names[i]));
		}
		LOG.debug("LingccFS list status of file:" + f);
		return results;	    
	}

	public void setWorkingDirectory(Path new_dir) {
		LOG.debug("Set working Dir =" + new_dir);
		workingDir = makeAbsolute(new_dir);
		checkPath(workingDir);
	}

	public Path getWorkingDirectory() {
		return workingDir;
	}

	/** Return the number of bytes that large input files should be optimally
	 * be split into to minimize i/o time. */
	public long getDefaultBlockSize() {
		return getConf().getLong("fs.lingccfs.block.size", 64*1024*1024);
	}



	public FileStatus getFileStatus(Path f) throws IOException {
		LOG.debug("LingccFS getFileStatus f=" + f);
		File path = pathToFile(f);
		LOG.debug("LingccFS getFileStatus path=" + path + ", path.exists()=" + path.exists());
		if (path.exists()) {
			return new RawLingccFileStatus(pathToFile(f), getDefaultBlockSize(), this);
		} else {
			throw new FileNotFoundException( "File " + f + " does not exist.");
		}
	}


	public String getPathName(Path f) throws IOException {
		checkPath(f);
		String result = makeAbsolute(f).toUri().toString();

		return result;
	}

	public void close() throws IOException {
		super.close();
	}

	/**
	 * The src file is on the local disk.  Add it to FS at
	 * the given dst name and the source is kept intact afterwards
	 */
	public void copyFromLocalFile(Path src, Path dst)
			throws IOException {
		copyFromLocalFile(false, src, dst);
	}

	/**
	 * The src files is on the local disk.  Add it to FS at
	 * the given dst name, removing the source afterwards.
	 */
	public void moveFromLocalFile(Path[] srcs, Path dst)
			throws IOException {
		copyFromLocalFile(true, true, srcs, dst);
	}

	/**
	 * The src file is on the local disk.  Add it to FS at
	 * the given dst name, removing the source afterwards.
	 */
	public void moveFromLocalFile(Path src, Path dst)
			throws IOException {
		copyFromLocalFile(true, src, dst);
	}

	/**
	 * The src file is on the local disk.  Add it to FS at
	 * the given dst name.
	 * delSrc indicates if the source should be removed
	 */
	public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
			throws IOException {
		copyFromLocalFile(delSrc, true, src, dst);
	}

	/**
	 * The src files are on the local disk.  Add it to FS at
	 * the given dst name.
	 * delSrc indicates if the source should be removed
	 */
	public void copyFromLocalFile(boolean delSrc, boolean overwrite, 
			Path[] srcs, Path dst)
					throws IOException {
		Configuration conf = getConf();
		LOG.info("LingccFS Copy From Local Files start. src:"+srcs + ", dst:" + dst
				+ ", delete src="+delSrc);
		FileUtil.copy(getLocal(conf), srcs, this, dst, delSrc, overwrite, conf);
		LOG.info("LingccFS Copy From Local Files end.");
	}

	/**
	 * The src file is on the local disk.  Add it to FS at
	 * the given dst name.
	 * delSrc indicates if the source should be removed
	 */
	public void copyFromLocalFile(boolean delSrc, boolean overwrite, 
			Path src, Path dst)
					throws IOException {
		Configuration conf = getConf();
		LOG.debug("LingccFS Copy From Local File start. src:"+src + ", dst:" + dst
				+ ", delete src="+delSrc);
		FileUtil.copy(getLocal(conf), src, this, dst, delSrc, overwrite, conf);
		LOG.debug("LingccFS Copy From Local File end.");
	}

	/**
	 * The src file is under FS, and the dst is on the local disk.
	 * Copy it from FS control to the local dst name.
	 */
	public void copyToLocalFile(Path src, Path dst) throws IOException {
		copyToLocalFile(false, src, dst);
	}

	/**
	 * The src file is under FS, and the dst is on the local disk.
	 * Copy it from FS control to the local dst name.
	 * Remove the source afterwards
	 */
	public void moveToLocalFile(Path src, Path dst) throws IOException {
		copyToLocalFile(true, src, dst);
	}

	/**
	 * The src file is under FS, and the dst is on the local disk.
	 * Copy it from FS control to the local dst name.
	 * delSrc indicates if the src will be removed or not.
	 */   
	public void copyToLocalFile(boolean delSrc, Path src, Path dst)
			throws IOException {
		LOG.debug("LingccFS Copy To Local File start. src:"+src + ", dst:" + dst
				+ ", delete src="+delSrc);
		FileUtil.copy(this, src, getLocal(getConf()), dst, delSrc, getConf());
		LOG.debug("LingccFS Copy To Local File end.");
	}


	static class RawLingccFileStatus extends FileStatus {
		/* We can add extra fields here. It breaks at least CopyFiles.FilePair().
		 * We recognize if the information is already loaded by check if
		 * onwer.equals("").
		 */
		private boolean isPermissionLoaded() {
			return !super.getOwner().equals(""); 
		}

		RawLingccFileStatus(File f, long defaultBlockSize, FileSystem fs) {
			super(f.length(), f.isDirectory(), 1, defaultBlockSize,
					f.lastModified(), new Path(f.getPath()).makeQualified(fs));
		}

		@Override
		public FsPermission getPermission() {
			if (!isPermissionLoaded()) {
				loadPermissionInfo();
			}
			return super.getPermission();
		}

		@Override
		public String getOwner() {
			if (!isPermissionLoaded()) {
				loadPermissionInfo();
			}
			return super.getOwner();
		}

		@Override
		public String getGroup() {
			if (!isPermissionLoaded()) {
				loadPermissionInfo();
			}
			return super.getGroup();
		}



		/// loads permissions, owner, and group from `ls -ld`
		private void loadPermissionInfo() {
			IOException e = null;
			try {
				StringTokenizer t = new StringTokenizer(
						execCommand(new File(getPath().toUri().getRawPath()), 
								Shell.getGET_PERMISSION_COMMAND()));
				//expected format
				//-rw-------    1 username groupname ...
				String permission = t.nextToken();
				if (permission.length() > 10) { //files with ACLs might have a '+'
					permission = permission.substring(0, 10);
				}
				//lingkun debug
				LOG.debug(" LingccFS Executed command:" + Shell.getGET_PERMISSION_COMMAND()
						+ ", on directory:" + getPath().getName());

				setPermission(FsPermission.valueOf(permission));
				t.nextToken();
				setOwner(t.nextToken());
				setGroup(t.nextToken());
			} catch (Shell.ExitCodeException ioe) {
				if (ioe.getExitCode() != 1) {
					e = ioe;
				} else {
					setPermission(null);
					setOwner(null);
					setGroup(null);
				}
			} catch (IOException ioe) {
				e = ioe;
			} finally {
				if (e != null) {
					throw new RuntimeException("Error while running command to get " +
							"file permissions : " + 
							StringUtils.stringifyException(e));
				}
			}
		}

		@Override
		public void write(DataOutput out) throws IOException {
			if (!isPermissionLoaded()) {
				loadPermissionInfo();
			}
			super.write(out);
		}
	}	

	/**
	 * Use the command chown to set owner.
	 */
	@Override
	public void setOwner(Path p, String username, String groupname
			) throws IOException {
		LOG.debug("LingccFS setOwner. Path="+ p + ", username="+ username
				+ ", groupname="+groupname);
		if (username == null && groupname == null) {
			throw new IOException("username == null && groupname == null");
		}

		if (username == null) {
			execCommand(pathToFile(p), Shell.SET_GROUP_COMMAND, groupname); 
		} else {
			//OWNER[:[GROUP]]
			String s = username + (groupname == null? "": ":" + groupname);
			execCommand(pathToFile(p), Shell.SET_OWNER_COMMAND, s);
		}
	}


	
	/**
	 * Use the command chmod to set permission.
	 */
	@Override
	public void setPermission(Path p, FsPermission permission
			) throws IOException {
		LOG.debug("LingccFS setPermission. Path="+p +", permission="
				+ permission.toString());
		FileUtil.setPermission(pathToFile(p), permission);
	}
	
	/**
	 * Used to expand relative, absolute path, absolute path with scheme to
	 * full LingccFS File Path, this is the absolute local path with lingccfs scheme.
	 * 
	 * It is called by makeQualified, pathToFile and make Absolute.
	 * @param path
	 * @return
	 */
	private Path expandToFullLingccPath(Path path) {
		Path fsPath = path;
		String pathStr = path.toString();
		long HBaseRootDirLen = (HBaseRootDir== null) ? 0:HBaseRootDir.toString().length();
	    if (!path.isAbsolute()) {
	   	  LOG.debug("add prefix:" + getWorkingDirectory() + " to existing path");	
	      fsPath = new Path(getWorkingDirectory().toUri().getPath(), path);
	    } else if( !path.toString().startsWith("lingccfs")) {
	    	// scheme without lingccfs, but seems absolute
	    	 if((HBaseRootDirLen > 0) 
	    			 && (HBaseRootDir.toString().length() > 0) 
	    			 && (!(HBaseRootDir.getPath().toString().startsWith(pathStr)
	    				  ||pathStr.startsWith(HBaseRootDir.getPath().toString())))
	    			 && (!(pathStr.startsWith(LingccFSWriteRoot.getPath().toString())))){
	    		 LOG.debug("add prefix:" + HBaseRootDir + " to existing path");
	    		fsPath = new Path(HBaseRootDir.getPath() + path);
	    	 } else if(!(pathStr.startsWith(LingccFSWriteRoot.getPath().toString())
	    			     || LingccFSWriteRoot.getPath().toString().startsWith(pathStr))) {
	    		 LOG.debug("add prefix:" + LingccFSWriteRoot + " to existing path");
			  	fsPath = new Path(LingccFSWriteRoot.getPath() + path);
	         } else {
	        	 LOG.debug("No prefix added: just keep the original path");
	        	fsPath = path;
	         }
	    } else{
	    	LOG.debug("No prefix added:start with lingccfs, only take the path of the URI.");
	    	fsPath = new Path(path.toUri().getPath());
	    }
	    LOG.debug("expand path:" + path.toString() +", to full:" + fsPath.toString()
	    		+", HBaseRootDir=" + ((HBaseRootDir== null) ? null:HBaseRootDir.getPath().toString())
	    		+ ", LingccFSWriteRoot=" + LingccFSWriteRoot.getPath().toString());
		return fsPath;
	}
	
	
	/**
	 *  Make the original /XXX path to lingccfs:///${fs.default.name}/XXX
	 */
	  @Override
	  public Path makeQualified(Path path) {
	    // make sure that we just get the 
	    // path component 
	    Path fsPath = expandToFullLingccPath(path);
	    URI tmpURI = fsPath.toUri();
	    //change this to Local uri 
	    fsPath = new Path(getUri().getScheme(), tmpURI.getPath());
	    LOG.debug("makeQualified of Path:" + path.toString() 
	    		+ ", return:" + fsPath.toString());
	    return fsPath;
	  }


		/** Convert a path to a File. */
		public File pathToFile(Path path) {
			checkPath(path);
			Path fsPath = expandToFullLingccPath(path);
			LOG.debug("Convert Path to file, Path:"+path.toString() + ". File:"
					+ fsPath.toString());
			return new File(fsPath.toString());
		}

		private Path makeAbsolute(Path f) {
			Path fsPath = expandToFullLingccPath(f);
			LOG.debug("MakeAbsolute, in Path f=" + f.toString() 
					+", return Path=" + fsPath.toString());
			return fsPath;
		}

}
