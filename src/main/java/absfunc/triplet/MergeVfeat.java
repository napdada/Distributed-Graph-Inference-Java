package absfunc.triplet;

import dataset.Vfeat;
import scala.runtime.AbstractFunction2;

import java.io.Serializable;
import java.util.HashMap;

/**
 * GraphOps.pregel 参数中 mergeMsg 的实现
 * 合并每轮迭代收到的 msg map
 * @author napdada
 * @version : v 0.1 2021/12/24 21:43
 */
public class MergeVfeat extends AbstractFunction2<HashMap<Long, Vfeat>, HashMap<Long, Vfeat>,
        HashMap<Long, Vfeat>> implements Serializable {
    /**
     * 合并 dst 收到的多个 map
     * @param map1 event subgraph2D feat map1
     * @param map2 event subgraph2D feat map2
     * @return map1、map2 并集
     */
    @Override
    public HashMap<Long, Vfeat> apply(HashMap<Long, Vfeat> map1, HashMap<Long, Vfeat> map2) {
        map1.putAll(map2);
        return map1;
    }
}
