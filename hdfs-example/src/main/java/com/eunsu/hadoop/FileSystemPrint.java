package com.eunsu.hadoop;
import java.net.URI;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileSystemPrint {
	public static void main(String[] args) throws IOException{
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);

		try(InputStream in = fs.open(new Path(uri))){
			IOUtils.copyBytes(in, System.out, 4096, false);
		}

	}
}
  
