package com.github.nisin.luigi.bayon;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.io.*;
import com.google.common.primitives.Doubles;
import com.sun.jndi.url.corbaname.corbanameURLContextFactory;
import org.apache.commons.exec.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

/**
 *
 * Created by Shoichi on 2014/02/06.
 */
public class Bayon {
    private final CommandLine cmd;
    private final boolean point;
    private final boolean clvector;
    private DefaultExecuteResultHandler handler;
    private FileBackedOutputStream out;
    private FileBackedOutputStream err;
    private Iterable<Clustor> result;



    public Bayon(CommandLine cmd, boolean point, boolean clvector) {
        this.cmd = cmd;
        this.point = point;
        this.clvector = clvector;
        handler = new DefaultExecuteResultHandler();

    }

    public Iterable<Clustor> getClusters() {
        waitFor();
        return result;

    }
    private static final class IterableLineReader implements Iterable<String> {
        private final CharSource source;
        public IterableLineReader(CharSource source) {
            this.source = source;
        }

        @Override
        public Iterator<String> iterator() {
            final Closer closer = Closer.create();
            try {
                final BufferedReader reader = source.openBufferedStream();
                return new AbstractIterator<String>() {
                    Splitter sp = Splitter.on("\t");
                    @Override
                    protected String computeNext() {
                        String line;
                        try {
                            line = reader.readLine();
                        } catch (IOException e) {
                            line = null;
                        }
                        if (line==null) {
                            try {
                                closer.close();
                            } catch (IOException ioe) {}
                            return endOfData();
                        }
                        return line;
                    }
                };
            } catch (Throwable e) {
                try {
                    closer.close();
                } catch (IOException ioe) {}
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    private void waitFor() {
        synchronized (this) {
            if (result==null) {
                try {
                    handler.waitFor();
                    if (err.asByteSource().size()>0)
                        System.out.println(err.asByteSource().asCharSource(Charsets.UTF_8).read());

                    Iterable<String> lines = new IterableLineReader(out.asByteSource().asCharSource(Charsets.UTF_8));

                    out = null;
                    final Iterable<Clustor> clustors = Iterables.transform(lines, new Function<String, Clustor>() {
                        Splitter sp = Splitter.on("\t");
                        @Override
                        public Clustor apply(String line) {
                            Iterator<String> cols_iter = sp.split(line).iterator();
                            Clustor clustor = new Clustor();
                            clustor.clustorId = Integer.valueOf( cols_iter.next());
                            clustor.documentIds = Maps.newHashMap();
                            while (cols_iter.hasNext()) {
                                String key = cols_iter.next();
                                Double val = point ? Double.valueOf(cols_iter.next()) : 1.0d ;
                                clustor.documentIds.put(key,val);
                            }
                            return clustor;
                        }
                    });
                    if (clvector) {
                        File vectorFile = (File) cmd.getSubstitutionMap().get("vector");
                        Iterable<String> vlines = new IterableLineReader(Files.asCharSource(vectorFile, Charsets.UTF_8));
                        final Iterable<Clustor> vectors = Iterables.transform(vlines, new Function<String, Clustor>() {
                            @Override
                            public Clustor apply(String line) {
                                Iterator<String> cols_iter = Splitter.on("\t").split(line).iterator();
                                Clustor clustor = new Clustor();
                                clustor.clustorId = Integer.valueOf( cols_iter.next());
                                clustor.vector = Maps.newHashMap();
                                while (cols_iter.hasNext()) {
                                    String key = cols_iter.next();
                                    Double val =  Double.valueOf(cols_iter.next());
                                    clustor.vector.put(key,val);
                                }
                                return clustor;
                            }
                        });
                        result = new Iterable<Clustor>() {
                            Iterable<Clustor> cs = clustors;
                            Iterable<Clustor> vs = vectors;
                            @Override
                            public Iterator<Clustor> iterator() {
                                return new AbstractIterator<Clustor>() {
                                    Iterator<Clustor> clustorsIter = cs.iterator();
                                    Iterator<Clustor> vectorsIter = vs.iterator();
                                    Clustor c =null;
                                    Clustor v =null;
                                    @Override
                                    protected Clustor computeNext() {
                                        if (clustorsIter.hasNext() ) {
                                            if (vectorsIter.hasNext()) {
                                                Clustor result ;
                                                if (c==null)
                                                    c = clustorsIter.next();
                                                if (v==null)
                                                    v = vectorsIter.next();
                                                if ( Objects.equal(c.clustorId,v.clustorId)) {
                                                    result = c;
                                                    result.vector = v.vector;
                                                    c = v = null;
                                                    return result;
                                                }
                                                else if (c.clustorId.compareTo( v.clustorId ) < 0) {
                                                    result = c;
                                                    c = null;
                                                }
                                                else {
                                                    result = v;
                                                    v = null;
                                                }
                                                return result;
                                            }
                                            else
                                                return clustorsIter.next();
                                        }
                                        else if (vectorsIter.hasNext())
                                            return vectorsIter.next();
                                        else
                                            return endOfData();
                                    }
                                };
                            }
                        };
//                        result = Iterables.mergeSorted(Lists.newArrayList(clustors,vectors),new Comparator<Clustor>() {
//
//                            @Override
//                            public int compare(Clustor o1, Clustor o2) {
//                                int cmp = o1.clustorId.compareTo(o2.clustorId);
//                                if (cmp==0) {
//                                    if (o1.vector==null)
//                                        o1.vector = o2.vector;
//                                    else if (o2.vector==null)
//                                        o2.vector = o1.vector;
//                                    if (o1.documentIds==null)
//                                        o1.documentIds = o2.documentIds;
//                                    else if (o2.documentIds==null)
//                                        o2.documentIds = o1.documentIds;
//                                }
//
//                                return cmp;
//                            }
//                        });
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
        out = new FileBackedOutputStream(2048);
        err = new FileBackedOutputStream(1024);
        exec.setStreamHandler(new PumpStreamHandler(out, err));
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
        public Integer clustorId;
        public Map<String,Double> documentIds;
        public Map<String,Double> vector;
    }

}
