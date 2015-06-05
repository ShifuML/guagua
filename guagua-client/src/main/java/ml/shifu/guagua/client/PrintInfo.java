/*
 * Copyright [2013-2015] PayPal Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ml.shifu.guagua.client;

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Print guagua version and help with logo.
 * 
 * @author Zhang David (pengzhang@paypal.com)
 */
public class PrintInfo {

    public static void main(String[] args) {
        printLogo();
        if(args.length == 1 && args[0].equals("v")) {
            printVersion();
        } else {
            printHelp();
        }
    }

    private static void printHelp() {
        System.out.println("Usage: guagua [-y(enable beta guagua-yarn version on hadoop 2.0)] "
                + "jar <the jar containing master and worker class> " + "[-D<hadoop/guagua property>] "
                + "<parameters to jar>");
    }

    private static void printLogo() {
        System.out.println("  ____ _   _   _    ____ _   _   _    ");
        System.out.println(" / ___| | | | / \\  / ___| | | | / \\   ");
        System.out.println("| |  _| | | |/ _ \\| |  _| | | |/ _ \\  ");
        System.out.println("| |_| | |_| / ___ \\ |_| | |_| / ___ \\ ");
        System.out.println(" \\____|\\___/_/   \\_\\____|\\___/_/   \\_\\");
        System.out.println("                                      ");
    }

    private static void printVersion() {
        String findContainingJar = findContainingJar(PrintInfo.class);
        JarFile jar = null;
        try {
            jar = new JarFile(findContainingJar);
            final Manifest manifest = jar.getManifest();

            String vendor = manifest.getMainAttributes().getValue("vendor");
            String title = manifest.getMainAttributes().getValue("title");
            String version = manifest.getMainAttributes().getValue("version");
            String timestamp = manifest.getMainAttributes().getValue("timestamp");
            System.out.println(vendor + " " + title + " version " + version + " \ncompiled " + timestamp);
        } catch (Exception e) {
            throw new RuntimeException("unable to read pigs manifest file", e);
        } finally {
            if(jar != null) {
                try {
                    jar.close();
                } catch (IOException e) {
                    throw new RuntimeException("jar closed failed", e);
                }
            }
        }
    }

    /**
     * Find a jar that contains a class of the same name, if any. It will return a jar file, even if
     * that is not the first thing on the class path that has a class with the same name.
     * 
     * @param my_class
     *            the class to find
     * @return a jar file that contains the class, or null
     * @throws IOException
     */
    @SuppressWarnings("rawtypes")
    private static String findContainingJar(Class my_class) {
        ClassLoader loader = my_class.getClassLoader();
        String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
        try {
            for(Enumeration itr = loader.getResources(class_file); itr.hasMoreElements();) {
                URL url = (URL) itr.nextElement();
                if("jar".equals(url.getProtocol())) {
                    String toReturn = url.getPath();
                    if(toReturn.startsWith("file:")) {
                        toReturn = toReturn.substring("file:".length());
                    }
                    // URLDecoder is a misnamed class, since it actually decodes
                    // x-www-form-urlencoded MIME type rather than actual
                    // URL encoding (which the file path has). Therefore it would
                    // decode +s to ' 's which is incorrect (spaces are actually
                    // either unencoded or encoded as "%20"). Replace +s first, so
                    // that they are kept sacred during the decoding process.
                    toReturn = toReturn.replaceAll("\\+", "%2B");
                    toReturn = URLDecoder.decode(toReturn, "UTF-8");
                    return toReturn.replaceAll("!.*$", "");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }
}
