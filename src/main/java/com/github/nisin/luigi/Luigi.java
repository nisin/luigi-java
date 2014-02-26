package com.github.nisin.luigi;

import com.github.nisin.luigi.bayon.Bayon;
import com.github.nisin.luigi.bayon.BayonService;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.*;
import com.google.common.io.Files;
import com.google.common.primitives.Doubles;

import java.io.*;
import java.util.*;

/**
 *
 * Created by Shoichi on 2014/02/10.
 */
public class Luigi {
    private static final double MINIMUM_SUGAR = 0.0000001;
    // アクセッサ
    private List<Node> tree;
    public List<Node> tree() { return tree; }
    public List<Node> tree(List<Node> tree) { this.tree = tree; return tree(); }
    private BayonService bayonService = BayonService.create()
            .with_idf(true)
            .with_limit(1.5)
            .with_clvector(true)
            .with_clvector_size(150);

    public static class DocVector implements Serializable {
        public String docId;
        public Map<String,Double> vector;
    }
    /**
     * 類似文書インデックス構築
     * @param vectors 文書ベクトル
     * @return
     */
    public List<Node> build(Iterable<DocVector> vectors) {
        List<Node> nodes = Lists.newArrayList();
        int i = 0;
        for (DocVector entry : vectors) {
            Node node = new Node();
            node.index = i;
            node.leaf = entry.docId;
            node.centroid = entry.vector;
            nodes.add(node);
            i++;
        }
        List<Node> tree = _stack_loop(nodes,0);
        return tree(tree);
    }

    /**
     * インデックスの保存
     * @throws IOException
     */
    public void save() throws IOException {
        File file = new File("store.bin");
        save(file);
    }

    /**
     * インデックスの保存
     * @param file 保存ファイル指定
     * @throws IOException
     */
    public void save(File file) throws IOException {
        save(Files.asByteSink(file).openStream());
    }

    /**
     * インデックスの保存
     * @param os 保存ストリーム
     * @throws IOException
     */
    public void save(OutputStream os) throws IOException {
        ObjectOutputStream oos = new ObjectOutputStream( os);
        oos.writeObject(tree);
    }

    /**
     * インデックスの読み込み
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public List<Node> load() throws IOException, ClassNotFoundException {
        File file = new File("store.bin");
        return load(file);
    }

    /**
     * ファイルからのインデックスの読み込み
     * @param file
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public List<Node> load(File file) throws IOException, ClassNotFoundException {
        return load(Files.asByteSource(file).openStream());
    }

    /**
     * ストリームからのインデックスの読み込み
     * @param is
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     */
    @SuppressWarnings("unchecked")
    public List<Node> load(InputStream is) throws IOException, ClassNotFoundException {
        ObjectInputStream ois = new ObjectInputStream(is);
        Object obj = ois.readObject();
        if ( obj instanceof List)
            return tree((List<Node>) obj);
        else
            throw new RuntimeException("Load Data INVALID that Not luigi-java");
    }

    private List<Node> _stack_loop(List<Node> nodeList, final int count) {
        int level = count + 1;
        final Set<String> word_counter = Sets.newHashSet();
        List<Bayon.Document> bayon_in = Lists.transform(nodeList, new Function<Node, Bayon.Document>() {
            @Override
            public Bayon.Document apply(Node node) {
                Bayon.Document document = new Bayon.Document();
                document.documentId = Integer.toString(node.index);
                document.vector = node.centroid;
                word_counter.addAll(node.centroid.keySet());
                return document;
            }
        });
        Bayon bayon = bayonService.clustering(bayon_in);
        bayon_in=null;
        Iterable<Bayon.Clustor> clustors = bayon.getClusters();
        bayon = null;
        List<Node> nodeList_new = Lists.newArrayList();
        int i = 0;
        for (Bayon.Clustor clustor : clustors) {
            Node node = new Node();
            node.index = i++;
            node.centroid = clustor.vector;
            for (String s :clustor.documentIds.keySet()) {
                int id = Integer.valueOf(s);
                Node child = nodeList.get(id);
                child.parent(node);
                node.add(child);
            }
            nodeList_new.add(node);
        }
        clustors=null;
        if (nodeList_new.size() > 10)
            return _stack_loop(nodeList_new,level);
        else
            return nodeList_new;
    }
    private static class PriorityNode {
        public double sim;
        public Node node;
        private PriorityNode( Node node,double sim) {
            this.sim = sim;
            this.node = node;
        }
    }

    /**
     * 類似文書を検索
     * @param in_vectors
     * @param num
     * @return
     */
    public List<Node> find(Map<String,Double> in_vectors,int num) {
        if (in_vectors.size()==0)
            return Lists.newArrayList();

        Map<String,Double> vectors = Maps.newHashMap(in_vectors);
        VectorTool.unit_length(vectors);

        // Rootノードをキューにいれておく
        PriorityQueue<PriorityNode> priority_queue = new PriorityQueue<PriorityNode>(num,new Comparator<PriorityNode>() {
            @Override
            public int compare(PriorityNode o1, PriorityNode o2) {
                return Doubles.compare(o1.sim,o2.sim);
            }
        });

        for (Node node : tree) {
            double sim = VectorTool.cosine_similarity(vectors,node.centroid);
            priority_queue.offer(new PriorityNode(node, 1 / (sim + MINIMUM_SUGAR)));
        }
        return _traverse(priority_queue,vectors,num);
    }

    private List<Node> _traverse(PriorityQueue<PriorityNode> queue, final Map<String, Double> wordset,final int num) {
        List<Node> result = Lists.newArrayList();
        while (queue.size()>0) {
            List<Node> traverses = Lists.newArrayList();
            while (queue.size()>0) {
                PriorityNode pnode = queue.poll();
                if (pnode.node.is_leaf()) {
                    if (result.size()>num)
                        break;
                    if (pnode.sim < (1/ MINIMUM_SUGAR)) {
                        for (Node child_node : pnode.node.child_nodes) {
                            double sim = VectorTool.cosine_similarity(wordset, child_node.centroid);
                            if (sim!=0.0)
                                result.add(child_node);
                        }
                    }
                }
                else {
                    traverses.add(pnode.node);
                    if (traverses.size()>=2) break;
                }
            }
            if (result.size()>num)
                break;
            else {
                for (Node node : traverses) {
                    for (Node child_node : node.child_nodes) {
                        double sim = VectorTool.cosine_similarity(wordset,child_node.centroid);
                        queue.offer(new PriorityNode(child_node,1/(sim+ MINIMUM_SUGAR)));
                    }
                }
            }
        }
        return Ordering.natural().reverse().onResultOf(new Function<Node, Comparable>() {
            @Override
            public Comparable apply(Node input) {
                return input.similarity;
            }
        }).sortedCopy(Iterables.transform(result, new Function<Node, Node>() {
            @Override
            public Node apply(Node node) {
                double sim = VectorTool.cosine_similarity(wordset, node.centroid);
                Node result_node = null;
                try {
                    result_node = node.clone();
                } catch (CloneNotSupportedException e) {
                    throw new RuntimeException(e);
                }
                result_node.similarity = sim;
                return result_node;
            }
        }));

    }


}
