package absfunc.triplet;

import scala.runtime.AbstractFunction2;

import java.io.Serializable;
import java.util.HashMap;

/**
 * GraphOps.pregel 形参中 mergeMsg 的实现
 * 将点收到的多个 embedding map 进行合并
 * @author napdada
 * @version : v 0.1 2021/12/25 11:14
 */
public class MergeEmb extends AbstractFunction2<HashMap<Long, float[]>, HashMap<Long, float[]>,
        HashMap<Long, float[]>> implements Serializable {
    /**
     * 一个二度子图的 embedding map 内容是一样的，只需选取 map1 或 map2
     * @param map1 embedding hashmap
     * @param map2 embedding hashmap
     * @return HashMap<Long, float[]> map1 或 map2
     */
    @Override
    public HashMap<Long, float[]> apply(HashMap<Long, float[]> map1, HashMap<Long, float[]> map2) {
        return map1.size() != 0 ? map1 : map2;
    }
}
