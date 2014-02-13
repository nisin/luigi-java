package com.github.nisin.luigi;

import com.google.common.base.Functions;
import com.google.common.base.Strings;
import com.google.common.collect.*;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.util.List;
import java.util.Map;

/**
 *
 * Created by Shoichi on 2014/02/10.
 */
public class Node implements Cloneable,Serializable {
    private static final Node EMPTY_NODE = new Node();
    public Map<String,Double> centroid;
    public List<Node> child_nodes = Lists.newArrayList();
    public String leaf;
    private transient WeakReference<Node> parent;
    public Double similarity;

    public Node parent() {
        return parent.get();
    }
    public Node parent(Node in_parent) {
        this.parent = new WeakReference<Node>(in_parent);
        return parent();
    }
    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
        stream.defaultReadObject();
        for (Node child_node : child_nodes) {
            child_node.parent(this);
        }
    }


    public void add (Node point) {
        child_nodes.add(point);
    }
    public boolean is_leaf() {
        return !Strings.isNullOrEmpty(Iterables.getFirst(child_nodes, EMPTY_NODE).leaf);
    }
    public List<Node> siblings() {
        return parent.get().child_nodes;
    }
    public Iterable<String> centroid_words() {
        return centroid_words(5);
    }

    public Iterable<String> centroid_words(int limit) {
        Ordering<String> sort = Ordering.natural().onResultOf(Functions.forMap(centroid)).reverse();
        return Iterables.limit(sort.sortedCopy(centroid.keySet()),limit);
    }

    public Node clone() {
        try {
            return (Node) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }
}
