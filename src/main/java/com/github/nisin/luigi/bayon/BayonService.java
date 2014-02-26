package com.github.nisin.luigi.bayon;

import com.google.common.base.*;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.FileBackedOutputStream;
import com.google.common.io.Files;
import com.sun.org.apache.bcel.internal.generic.RET;
import org.apache.commons.exec.CommandLine;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Created by Shoichi on 2014/02/06.
 */
public class BayonService {
    static File work_dir = Files.createTempDir();
    private static final String BAYON_BASENAME = "bayon";
    public BayonService() {
        this.bayon_path = BAYON_BASENAME;
    }

    private String bayon_path;
    public static BayonService create() {
        BayonService service = new BayonService();

        for (String path : Lists.newArrayList("/bin","/usr/bin","/usr/local/bin","/opt/bin")) {
            File bin = new File(path,BAYON_BASENAME);
            if (bin.exists()) {
                service.bayon_path = bin.getAbsolutePath();
                break;
            }
        };
        return service;
    }
    public static BayonService create(String commandPath) {
        BayonService service = create();
        service.bayon_path = commandPath;
        return service;
    }

    public Bayon clustering(Iterable<Bayon.Document> documents) {
        try {
            File input = work_dir.createTempFile("bayon_document","input");
            File vector = work_dir.createTempFile("bayon_clvector","output");
            Files.asCharSink(input,Charsets.UTF_8).writeLines(Iterables.transform(documents,new Function<Bayon.Document, CharSequence>() {
                Joiner.MapJoiner mj = Joiner.on("\t").withKeyValueSeparator("\t");
                StringBuilder sb = new StringBuilder();
                @Override
                public CharSequence apply(Bayon.Document input) {
                    sb.setLength(0);
                    sb.append(input.documentId).append("\t");
                    mj.appendTo(sb,input.vector);
                    return sb.toString();
                }
            }));

            CommandLine cmd = CommandLine.parse(bayon_path);
            Map<String,File> map = Maps.newHashMap();

            if (number!=null)
                cmd.addArgument("-n").addArgument(number().toString());
            else if (limit!=null)
                cmd.addArgument("-l").addArgument(limit().toString());
            else if (number==null && limit==null)
                cmd.addArgument("-l").addArgument("1.5");
            if (point)
                cmd.addArgument("-p");
            if (clvector) {
                cmd.addArgument("-c").addArgument("${vector}");
                map.put("vector",vector);
                if (clvector_size!=null)
                    cmd.addArgument("--clvector-size").addArgument(clvector_size.toString());
            }
            if (method!=null)
                cmd.addArgument("--method").addArgument(method.toString());
            if (seed!=null)
                cmd.addArgument("--seed").addArgument(seed.toString());
            if (idf)
                cmd.addArgument("--idf");
            cmd.addArgument("${input}");
            map.put("input",input);
            if (map.size()>0)
                cmd.setSubstitutionMap(map);
            Bayon bayon = new Bayon(cmd,point,clvector);
            bayon.process();
            return bayon;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Integer number;
    public Integer number()  {return number; }
    public Integer number(Integer number) { this.number = number; return number(); }
    public BayonService with_number(Integer number) { number(number); return this; }
    private Double limit;
    public Double limit()  {return limit; }
    public Double limit(Double limit) { this.limit = limit; return limit(); }
    public BayonService with_limit(Double limit) { limit(limit); return this; }
    private boolean point;
    public boolean point()  {return point; }
    public boolean point(boolean point) { this.point = point; return point(); }
    public BayonService with_point(boolean point) { point(point); return this; }
    private boolean clvector;
    public boolean clvector()  {return clvector; }
    public boolean clvector(boolean clvector) { this.clvector = clvector; return clvector(); }
    public BayonService with_clvector(boolean clvector) { clvector(clvector); return this; }
    private Integer clvector_size;
    public Integer clvector_size()  {return clvector_size; }
    public Integer clvector_size(Integer clvector_size) { this.clvector_size = clvector_size; return clvector_size(); }
    public BayonService with_clvector_size(Integer clvector_size) { clvector_size(clvector_size); return this; }
    public enum METHOD {rb,kmeans}
    private METHOD method;
    public METHOD method()  {return method; }
    public METHOD method(METHOD method) { this.method = method; return method(); }
    public BayonService with_method(METHOD method) { method(method); return this; }
    private Integer seed;
    public Integer seed()  {return seed; }
    public Integer seed(Integer seed) { this.seed = seed; return seed(); }
    public BayonService with_seed(Integer seed) { seed(seed); return this; }
    private boolean idf;
    public boolean idf()  {return idf; }
    public boolean idf(boolean idf) { this.idf = idf; return idf(); }
    public BayonService with_idf(boolean idf) { idf(idf); return this; }

    private File classify; // # required
    public File classify()  {return classify; }
    public File classify(File classify) { this.classify = classify; return classify(); }
    public BayonService with_classify(File classify) { classify(classify); return this; }
    private Integer inv_keys = 20;
    public Integer inv_keys()  {return inv_keys; }
    public Integer inv_keys(Integer inv_keys) { this.inv_keys = inv_keys; return inv_keys(); }
    public BayonService with_inv_keys(Integer inv_keys) { inv_keys(inv_keys); return this; }
    private Integer inv_size = 100;
    public Integer inv_size()  {return inv_size; }
    public Integer inv_size(Integer inv_size) { this.inv_size = inv_size; return inv_size(); }
    public BayonService with_inv_size(Integer inv_size) { inv_size(inv_size); return this; }
    private Integer classify_size = 100;
    public Integer classify_size()  {return classify_size; }
    public Integer classify_size(Integer classify_size) { this.classify_size = classify_size; return classify_size(); }
    public BayonService with_classify_size(Integer classify_size) { classify_size(classify_size); return this; }


}
