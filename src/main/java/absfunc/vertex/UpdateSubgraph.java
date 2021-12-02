package absfunc.vertex;

import dataset.Vdata;
import dataset.Vfeat;
import lombok.Getter;
import lombok.Setter;
import scala.Option;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * @author napdada
 * @version : v 0.1 2021/11/24 7:47 下午
 */
@Getter
@Setter
public class UpdateSubgraph extends AbstractFunction3<Object, Vdata, Option<Vdata>, Vdata> implements Serializable {
    private int turnNum;
    private String name;

    public UpdateSubgraph(int turnNum, String name) {
        this.turnNum = turnNum;
        this.name = name;
    }

    @Override
    public Vdata apply(Object vID, Vdata v, Option<Vdata> newV) {
        if (!newV.isEmpty()) {
            if (name.equals("vertex")) {
                HashSet<String> subgraph2D = null;
                HashMap<Long, Vfeat> subgraph2DFeat = null;
                if (turnNum == 1) {
                    Long id;
                    Vfeat vfeat;
                    subgraph2D = new HashSet<>();
                    subgraph2DFeat = new HashMap<>();
                    for (Map.Entry<Long, Vfeat> entry : newV.get().getSubgraph2DFeat().entrySet()) {
                        id = entry.getKey();
                        vfeat = entry.getValue();
                        subgraph2D.add(String.valueOf(id));
                        subgraph2D.add(vID + "-" + id);
                        subgraph2D.add(id + "-" + vID);
                        subgraph2DFeat.put(id, vfeat);
                    }
                } else if (turnNum == 2) {
                    subgraph2D = mergeSet(v.getSubgraph2D(), newV.get().getSubgraph2D());
                    subgraph2DFeat = mergeMap(v.getSubgraph2DFeat(), newV.get().getSubgraph2DFeat());
                }
                return new Vdata((Long) vID, v, subgraph2D, subgraph2DFeat, v.getEventSubgraph2D(), v.getEventSubgraph2DFeat());
            } else if (name.equals("event")) {
                HashSet<String> eventSubgraph2D;
                HashMap<Long, Vfeat> eventSubgraph2DFeat = v.getEventSubgraph2DFeat();
                if (turnNum == 0) {
                    eventSubgraph2D = mergeSet(v.getSubgraph2D(), newV.get().getSubgraph2D());
                    eventSubgraph2DFeat = mergeMap(v.getSubgraph2DFeat(), newV.get().getSubgraph2DFeat());
                } else {
                    eventSubgraph2D = mergeSet(v.getEventSubgraph2D(), newV.get().getEventSubgraph2D());
                }
                return new Vdata((Long) vID, v, v.getSubgraph2D(), v.getSubgraph2DFeat(), eventSubgraph2D, eventSubgraph2DFeat);
            } else if (name.equals("embedding")) {
                HashMap<Long, float[]> embedding = newV.get().getEmbedding();
                float[] feat = embedding.get((Long) vID);
                return new Vdata((Long) vID, v, feat, embedding);
            }
        }
        return v;
    }

    public HashSet<String> mergeSet(HashSet<String> set1, HashSet<String> set2) {
        HashSet<String> set = new HashSet<>();
        set.addAll(set1);
        set.addAll(set2);
        return set;
    }

    public HashMap<Long, Vfeat> mergeMap(HashMap<Long, Vfeat> map1, HashMap<Long, Vfeat> map2) {
        HashMap<Long, Vfeat> map = new HashMap<>();
        map.putAll(map1);
        map.putAll(map2);
        return map;
    }
}
