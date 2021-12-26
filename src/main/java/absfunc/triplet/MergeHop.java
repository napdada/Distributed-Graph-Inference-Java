package absfunc.triplet;

import scala.runtime.AbstractFunction2;

import java.io.Serializable;

/**
 * GraphOps.pregel 形参中 mergeMsg 的实现
 * 每轮迭代收到的 msg 中选择最大的 hop
 * @author napdada
 * @version : v 0.1 2021/12/23 10:57
 */
public class MergeHop extends AbstractFunction2<Integer, Integer, Integer> implements Serializable {
    /**
     * 合并 hop 时选择最大的 hop 值
     * @param hop1 vdata hop 值
     * @param hop2 vdata hop 值
     * @return max(hop1, hop2)
     */
    @Override
    public Integer apply(Integer hop1, Integer hop2) {
        return hop1 > hop2 ? hop1 : hop2;
    }
}
