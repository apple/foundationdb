/*
 * JNIUtil.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Utility for loading a dynamic library from the classpath.
 *
 */
public class JNIUtil {
	private static final String SEPARATOR = "/";
	private static final String LOADABLE_PREFIX = "FDB_LIBRARY_PATH_";
	private static final String TEMPFILE_PREFIX = "fdbjni";
	private static final String TEMPFILE_SUFFIX = ".library";

	private static class OS {
		private final String name;
		private final String arch;
		private final boolean canDeleteEager;

		OS(String name, String arch, boolean canDeleteEager) {
			this.name = name;
			this.arch = arch;
			this.canDeleteEager = canDeleteEager;
		}

		public String getName() {
			return this.name;
		}

		public String getArch() {
			return this.arch;
		}
	}

	/**
	 * Attempts a platform specific load of a library using classpath resources. In
	 *  the case that the load fails, a call to {@link System#loadLibrary(String)}
	 *  will be made as a way to use a library not included in a jar.
	 *
	 * @param libName the name of the library to attempt to load. This name should be
	 *  undecorated with file extensions and, in the case of *nix, "lib" prefixes.
	 */
	static void loadLibrary(String libName) throws UnsatisfiedLinkError {
		if(libName == null) {
			throw new NullPointerException("Library name must not be null");
		}

		String libPathToLoad = null;
		try {
			String prop = LOADABLE_PREFIX + libName.toUpperCase();
			libPathToLoad = System.getProperty(prop);
		} catch(SecurityException e) {
			// eat
		}

		if(libPathToLoad != null) {
			System.load(libPathToLoad);
			return;
		}

		OS os = getRunningOS();
		String path = getPath(os, libName);

		if ((os.getName().equals("linux") && !path.endsWith(".so")) || (os.getName().equals("windows") && !path.endsWith(".dll")) || (os.getName().equals("osx") && !path.endsWith(".jnilib") && !path.endsWith(".dylib"))) {
			throw new IllegalStateException("OS sanity check failed. System property os.name reports " + os.getName()+" but System.mapLibraryName is looking for " + getLibName(libName));
		}

		File exported;

		try {
			exported = exportResource(path, libName);
		}
		catch (IOException e) {
			throw new UnsatisfiedLinkError(e.getMessage());
		}
		String filename = exported.getAbsolutePath();

		System.load(filename);
		if(os.canDeleteEager) {
			try {
				exported.delete();
			} catch(Throwable t) {
				// EAT, since we do not care that an eager deletion did not work...
			}
		}
	}

	/**
	 * Export a library from classpath resources to a temporary file.
	 *
	 * @param libName the name of the library to attempt to export. This name should be
	 *  undecorated with file extensions and, in the case of *nix, "lib" prefixes.
	 * @return the exported temporary file
	 */
	public static File exportLibrary(String libName) throws IOException {
		OS os = getRunningOS();
		String path = getPath(os, libName);
		return exportResource(path, libName);
	}

	/**
	 * Gets a relative path for a library. The path will be of the form:
	 *  {@code {os}/{arch}/{name}}.
	 *
	 * @return a relative path to a resource to be loaded from the classpath
	 */
	private static String getPath(OS os, String libName) {
		return SEPARATOR +
				"lib" + SEPARATOR +
				os.getName() + SEPARATOR +
				os.getArch() + SEPARATOR +
				getLibName(libName);
	}

	/**
	 * Export a resource from the classpath to a temporary file.
	 *
	 * @param path the relative path of the file to load from the classpath
	 * @param name an optional descriptive name to include in the temporary file's path
	 *
	 * @return the absolute path to the exported file
	 * @throws IOException
	 */
	private static File exportResource(String path, String name) throws IOException {
		InputStream resource = JNIUtil.class.getResourceAsStream(path);
		if(resource == null)
			throw new IllegalStateException("Embedded library jar:" + path + " not found");
		File f = saveStreamAsTempFile(resource, name);
		return f;
	}

	private static File saveStreamAsTempFile(InputStream resource, String name) throws IOException {
		File f = File.createTempFile(name.length() > 0 ? name : TEMPFILE_PREFIX, TEMPFILE_SUFFIX);
		FileOutputStream outputStream = new FileOutputStream(f);
		copyStream(resource, outputStream);
		outputStream.flush();
		outputStream.close();
		f.deleteOnExit();
		return f;
	}

	private static void copyStream(InputStream resource, OutputStream fileOutputStream) throws IOException {
		byte[] buffer = new byte[4096];
		int bytesRead;
		while((bytesRead = resource.read(buffer)) > 0) {
			fileOutputStream.write(buffer, 0, bytesRead);
		}
	}

	private static String getLibName(String libName) {
		String systemLibName = System.mapLibraryName(libName);
		if (systemLibName.endsWith(".dylib")) {
			systemLibName = systemLibName.replace("dylib", "jnilib");
		}
		return systemLibName;
	}

	private static OS getRunningOS() {
		String osname = System.getProperty("os.name").toLowerCase();
		String arch = System.getProperty("os.arch");
		if (!arch.equals("amd64") && !arch.equals("x86_64") && !arch.equals("aarch64") && !arch.equals("ppc64le")) {
			throw new IllegalStateException("Unknown or unsupported arch: " + arch);
		}
		if (osname.startsWith("windows")) {
			return new OS("windows", arch, /* canDeleteEager */ false);
		} else if (osname.startsWith("linux")) {
			return new OS("linux", arch, /* canDeleteEager */ true);
		} else if (osname.startsWith("mac") || osname.startsWith("darwin")) {
			return new OS("osx", arch, /* canDeleteEager */ true);
		} else {
			throw new IllegalStateException("Unknown or unsupported OS: " + osname);
		}
	}

	private JNIUtil() {}
}
