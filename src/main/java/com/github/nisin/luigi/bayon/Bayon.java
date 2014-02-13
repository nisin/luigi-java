package com.github.nisin.luigi.bayon;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.io.*;
import com.google.common.primitives.Doubles;
import org.apache.commons.exec.*;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by Shoichi on 2014/02/06.
 */
public class Bayon {
    private final CommandLine cmd;
    private final boolean point;
    private final boolean clvector;
    private DefaultExecuteResultHandler handler;
    private FileBackedOutputStream out;
    private Iterable<Clustor> result;



    public Bayon(CommandLine cmd, boolean point, boolean clvector) {
        this.cmd = cmd;
        this.point = point;
        this.clvector = clvector;
        handler = new DefaultExecuteResultHandler();

    }

    public boolean running() {
        return true;
    }
    public Iterable<Clustor> getClusters() {
        waitFor();
        return result;

    }

    private void waitFor() {
        synchronized (this) {
            if (handler.hasResult() && result!=null) {
                try {
                    handler.waitFor();
                    File vectorFile = (File) cmd.getSubstitutionMap().get("vector");
                    List<String> lines = out.asByteSource().asCharSource(Charsets.UTF_8).readLines();
                    out = null;
                    Iterable<Clustor> clustors = Iterables.transform(lines, new Function<String, Clustor>() {
                        Splitter sp = Splitter.on("\t");
                        Converter<String,Double> dc = Doubles.stringConverter();
                        @Override
                        public Clustor apply(String line) {
                            Iterator<String> cols_iter = sp.split(line).iterator();
                            Clustor clustor = new Clustor();
                            clustor.clustorId = cols_iter.next();
                            clustor.documentIds = Maps.newHashMap();
                            while (cols_iter.hasNext()) {
                                String key = cols_iter.next();
                                Double val = point ? dc.convert(cols_iter.next()) : 1.0d ;
                                clustor.documentIds.put(key,val);
                            }
                            return clustor;
                        }
                    });
                    if (clvector) {
                        List<String> vlines = Files.asCharSource(vectorFile, Charsets.UTF_8).readLines();
                        Iterable<Clustor> vectors = Iterables.transform(vlines, new Function<String, Clustor>() {
                            @Override
                            public Clustor apply(String line) {
                                Iterator<String> cols_iter = Splitter.on("\t").split(line).iterator();
                                Clustor clustor = new Clustor();
                                clustor.clustorId = cols_iter.next();
                                clustor.vector = Maps.newHashMap();
                                while (cols_iter.hasNext()) {
                                    String key = cols_iter.next();
                                    Double val =  Doubles.stringConverter().convert(cols_iter.next());
                                    clustor.vector.put(key,val);
                                }
                                return clustor;
                            }
                        });
                        result = Iterables.mergeSorted(Lists.newArrayList(clustors,vectors),new Comparator<Clustor>() {

                            @Override
                            public int compare(Clustor o1, Clustor o2) {
                                int cmp = o1.clustorId.compareTo(o2.clustorId);
                                if (cmp==0) {
                                    if (o1.vector==null)
                                        o1.vector = o2.vector;
                                    else if (o2.vector==null)
                                        o2.vector = o1.vector;
                                    if (o1.documentIds==null)
                                        o1.documentIds = o2.documentIds;
                                    else if (o2.documentIds==null)
                                        o2.documentIds = o1.documentIds;
                                }

                                return cmp;
                            }
                        });
                    }
                    else
                        result = clustors;

                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }



    public void process() {
        Executor exec = new DefaultExecutor();
        out = new FileBackedOutputStream(1024*1024);
        exec.setStreamHandler(new PumpStreamHandler(out, ByteStreams.nullOutputStream()));
//        out.asByteSource().asCharSource();
        try {
            exec.execute(cmd, handler);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class Document {
        public String documentId;
        public Map<String,Double> vector;
    }
    public static class Clustor {
        public String clustorId;
        public Map<String,Double> documentIds;
        public Map<String,Double> vector;
    }

}
