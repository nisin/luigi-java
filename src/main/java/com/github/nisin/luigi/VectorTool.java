package com.github.nisin.luigi;

import java.util.Map;

/**
 * Created by Shoichi on 2014/02/10.
 */
public class VectorTool {
    public static void unit_length(Map<String,Double> vec) {
        double norm = norm(vec);
        for (Map.Entry<String, Double> entry : vec.entrySet()) {
            double scalar = entry.getValue();
            entry.setValue(scalar/norm);
        }
    }

    public static double cosine_similarity(Map<String,Double> vec1, Map<String,Double> vec2) {
        double inner_product = 0.0d;
        for (String key : vec1.keySet()) {
            if (vec2.containsKey(key)) {
                inner_product += vec1.get(key) * vec2.get(key);
            }
        }
        double norm1 = 0.0d;
        for (Double scalar : vec1.values()) { norm1 += Math.pow(scalar,2); }
        norm1 = Math.sqrt(norm1);
        double norm2 = 0.0d;
        for (Double scalar : vec2.values()) { norm2 += Math.pow(scalar,2); }
        norm2 = Math.sqrt(norm2);
        return (norm1>0d && norm2>0d) ? inner_product /(norm1*norm2) : 0.0d;
    }


    private static double norm(Map<String, Double> vec) {
        double norm = 0d;
        for (Double scalar : vec.values()) {
            norm += Math.pow(scalar,2d);
        }
        return Math.sqrt(norm);
    }
}
