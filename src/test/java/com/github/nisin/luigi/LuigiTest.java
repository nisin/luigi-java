package com.github.nisin.luigi;

import com.carrotsearch.sizeof.RamUsageEstimator;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import junit.framework.TestCase;

import java.io.File;
import java.net.URL;
import java.util.*;

/**
 *
 * Created by Shoichi on 2014/02/20.
 */
public class LuigiTest extends TestCase {
    public void testTree() throws Exception {
        Luigi luigi = new Luigi();
        List<Node> tree = Lists.newArrayList();
        luigi.tree(tree);
        assertSame(tree,luigi.tree());
    }

    public void testBuild() throws Exception {
        Luigi luigi = new Luigi();
        URL input_txt = Resources.getResource(LuigiTest.class,"input.txt");
        List<Luigi.DocVector> docVectors = Lists.transform(Files.readLines(new File(input_txt.getPath()), Charsets.UTF_8), new Function<String, Luigi.DocVector>() {
            @Override
            public Luigi.DocVector apply(String input) {
                Iterable<String> fs = Splitter.on("\t").split(input);
                Iterator<String> fsi = fs.iterator();
                Luigi.DocVector vector = new Luigi.DocVector();
                vector.docId = fsi.next();
                vector.vector = Maps.newHashMap();
                while (fsi.hasNext()) {
                    String key = fsi.next();
                    if (fsi.hasNext()) {
                        Double val = Double.valueOf(fsi.next());
                        vector.vector.put( key, val );
                    }
                }
                return vector;
            }
        });
        luigi.build(docVectors);
        long size = RamUsageEstimator.sizeOf(luigi.tree())/1024/1024;
        System.out.println("RamUsage: "+size+"m");

    }

    public void testSave() throws Exception {

    }

    public void testLoad() throws Exception {

    }

    public void testFind() throws Exception {
        Luigi luigi = new Luigi();
        URL input_txt = Resources.getResource(LuigiTest.class,"input.txt");
        List<Luigi.DocVector> docVectors = Lists.transform(Files.readLines(new File(input_txt.getPath()), Charsets.UTF_8), new Function<String, Luigi.DocVector>() {
            @Override
            public Luigi.DocVector apply(String input) {
                Iterable<String> fs = Splitter.on("\t").split(input);
                Iterator<String> fsi = fs.iterator();
                Luigi.DocVector vector = new Luigi.DocVector();
                vector.docId = fsi.next();
                vector.vector = Maps.newHashMap();
                while (fsi.hasNext()) {
                    String key = fsi.next();
                    if (fsi.hasNext()) {
                        Double val = Double.valueOf(fsi.next());
                        vector.vector.put( key, val );
                    }
                }
                return vector;
            }
        });
        luigi.build(docVectors);
        Deque<List<Node>> queue = Lists.newLinkedList();
        queue.push(Lists.newArrayList(luigi.tree()));
        while ( queue.size()>0 ) {
            List<Node> nodes = queue.peek();
            if (nodes.size()>0) {
                Node node = nodes.remove(0);
                System.out.println(Strings.repeat("  +",queue.size()) + node);
                queue.push(Lists.newArrayList(node.child_nodes));
            }
            else
                queue.pop();
        }


        Map<String,Double> term = Maps.newHashMap();
        term.put("アナウンサー",1.0);
        term.put("関西",1.0);
        List<Node> founds = luigi.find(term, 30);
        for (Node found : founds) {
            System.out.println(found.toString());
        }
        term = Maps.newHashMap();
        term.put("双葉社",1.0);
        term.put("未来",1.0);
        founds = luigi.find(term, 30);
        for (Node found : founds) {
//            System.out.println("" + found.similarity.toString() +" "+found.leaf +" "+ found.centroid_words().toString());
            System.out.println(found.toString());
        }

    }
}
