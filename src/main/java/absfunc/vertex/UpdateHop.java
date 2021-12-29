package absfunc.vertex;

import dataset.Vdata;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;

/**
 * GraphOps.pregel 形参中 vprog 的实现
 * 将 pregel 每轮迭代 hop msg 更新到点上
 * @author napdada
 * @version : v 0.1 2021/12/23 10:55
 */
public class UpdateHop extends AbstractFunction3<Object, Vdata, Integer, Vdata> implements Serializable {
    /**
     * 更新点的 hop
     * @param vID 点 ID
     * @param v 点
     * @param hop 跳数（0、1、2）
     * @return Vdata 更新 hop 后的点
     */
    @Override
    public Vdata apply(Object vID, Vdata v, Integer hop) {
        Vdata newV = new Vdata((Long) vID, v);
        // 新 hop 比旧 hop 大才更新（不包含 init hop = 2 的情况）
        if (v.getHop() == 2 || v.getHop() < hop) {
            newV.setHop(hop);
        } else {
            newV.setHop(v.getHop());
        }
        return newV;
    }
}
