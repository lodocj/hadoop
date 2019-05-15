/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

/**
 * Sort split,data path is from alluxio,memory priority
 */
public class InputSplitSorter {

	private final Class<?> alluxioURICls;
	private final Class<?> baseFileSystemCls;
	private final Method getStatusMethod;
	private final Method getInAlluxioPercentageMethod;
	private final Method getInMemPercentageMethod;
	private final JobConf conf;
	private final String ALLUXIO_SCHEMA = "alluxio";
	private Map<Key, Object> fsCache = Maps.newHashMap();
	final Map<Path, Integer> memCache = Maps.newHashMap();
	final Map<Path, Integer> alluxioCache = Maps.newHashMap();

	static class Key {
		final String scheme;
		final String authority;

		Key(String scheme, String authority) {
			this.scheme = scheme;
			this.authority = authority;
		}

		@Override
		public int hashCode() {
			return (scheme + authority).hashCode();
		}

		static boolean isEqual(Object a, Object b) {
			return a == b || (a != null && a.equals(b));
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			}
			if (obj != null && obj instanceof Key) {
				Key that = (Key) obj;
				return isEqual(this.scheme, that.scheme) && isEqual(this.authority, that.authority);
			}
			return false;
		}

		@Override
		public String toString() {
			return scheme + "://" + authority;
		}
	}

	public InputSplitSorter(JobConf jobConf) {
		try {
			this.conf = jobConf;
			this.alluxioURICls = Class.forName("alluxio.AlluxioURI");
			this.baseFileSystemCls = Class.forName("alluxio.client.file.BaseFileSystem");
			this.getStatusMethod = baseFileSystemCls.getMethod("getStatus", alluxioURICls);
			Class<?> uriStatusCls = Class.forName("alluxio.client.file.URIStatus");
			this.getInAlluxioPercentageMethod = uriStatusCls.getDeclaredMethod("getInAlluxioPercentage");
			this.getInMemPercentageMethod = uriStatusCls.getDeclaredMethod("getInMemoryPercentage");
		} catch (Exception ex) {
			throw new RuntimeException("reference alluxio not found!" + ex.getMessage());
		}
	}

	public void sortSplits(InputSplit[] splits) throws IOException {
		Arrays.sort(splits, new Comparator<org.apache.hadoop.mapred.InputSplit>() {
			public int compare(org.apache.hadoop.mapred.InputSplit a, org.apache.hadoop.mapred.InputSplit b) {
				try {
					if (a instanceof CombineFileSplit && b instanceof CombineFileSplit) {
						CombineFileSplit leftSplit = (CombineFileSplit) a;
						CombineFileSplit rightSplit = (CombineFileSplit) b;
						Path[] pathsLeft = leftSplit.getPaths();
						Path[] pathsRight = rightSplit.getPaths();
						String schemaLeft = null, schemaRight = null;

						if (pathsLeft.length > 0) {
							schemaLeft = pathsLeft[0].toUri().getScheme();
						}
						if (pathsRight.length > 0) {
							schemaRight = pathsRight[0].toUri().getScheme();
						}
						if (ALLUXIO_SCHEMA.equals(schemaLeft) && !ALLUXIO_SCHEMA.equals(schemaRight)) {
							return 1;
						}

						if (!ALLUXIO_SCHEMA.equals(schemaLeft) && ALLUXIO_SCHEMA.equals(schemaRight)) {
							return -1;
						}

						if (ALLUXIO_SCHEMA.equals(schemaLeft) && ALLUXIO_SCHEMA.equals(schemaRight)) {
							int inMemTotalLeft = getInMemTotal(pathsLeft);
							int inMemTotalRight = getInMemTotal(pathsRight);
							int flag = Ints.compare(inMemTotalRight, inMemTotalLeft);
							if (flag != 0) {
								return flag;
							} else {
								int inAlluxioLeft = getInAlluxioTotal(leftSplit.getPaths());
								int inAlluxioRight = getInAlluxioTotal(rightSplit.getPaths());
								return Ints.compare(inAlluxioRight, inAlluxioLeft);
							}
						} else {
							return Longs.compare(b.getLength(), a.getLength());
						}
					} else {
						return Longs.compare(b.getLength(), a.getLength());
					}
				} catch (IOException ioe) {
					throw new RuntimeException("Problem sorting input splits", ioe);
				}
			}
		});
	}

	private int getInMemTotal(Path[] paths) throws IOException {
		int total = 0;
		for (Path path : paths) {
			Integer p = memCache.get(path);
			if (p == null) {
				p = getInMemPercentSingle(path);
				memCache.put(path, p);
			}
			total += p;
		}
		return total;
	}

	private int getInAlluxioTotal(Path[] paths) throws IOException {
		int total = 0;
		for (Path path : paths) {
			Integer p = alluxioCache.get(path);
			if (p == null) {
				p = getInAlluxioPercent(path);
				alluxioCache.put(path, p);
			}
			total += p;
		}
		return total;
	}

	public int getInAlluxioPercent(Path path) {
		try {
			if(!ALLUXIO_SCHEMA.equals(path.toUri().getScheme())){
				return 100;
			}
			Object uriStatus = getStatusMethod.invoke(getBaseFileSystem(path), buildAlluxioUri(path.toString()));
			return (int) getInAlluxioPercentageMethod.invoke(uriStatus);
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
	}

	public Object getBaseFileSystem(Path path) {
		try {
			Key key = new Key(path.toUri().getScheme(), path.toUri().getAuthority());
			Object baseFileSystem = fsCache.get(key);
			if (baseFileSystem == null) {
				FileSystem fs = path.getFileSystem(conf);
				Field publicField = fs.getClass().getSuperclass().getDeclaredField("mFileSystem");
				publicField.setAccessible(true);
				baseFileSystem = publicField.get(fs);
				fsCache.put(key, baseFileSystem);
			}
			return baseFileSystem;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public int getInMemPercentSingle(Path path) {
		try {
			if(!ALLUXIO_SCHEMA.equals(path.toUri().getScheme())){
				return 100;
			}
			Object uriStatus = getStatusMethod.invoke(getBaseFileSystem(path), buildAlluxioUri(path.toString()));
			return (int) getInMemPercentageMethod.invoke(uriStatus);
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
	}

	public Object buildAlluxioUri(String path) throws Exception {
		Constructor<?> con = alluxioURICls.getConstructor(String.class);
		Object ret = con.newInstance(path);
		return ret;
	}
}
