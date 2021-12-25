package absfunc.vertex;

import dataset.Vdata;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.HashMap;

/**
 * GraphOps.pregel 形参中 vprog 的实现
 * 将 pregel 每轮迭代 merge msg 更新到点上
 * @author napdada
 * @version : v 0.1 2021/12/25 11:33
 */
public class UpdateEmb extends AbstractFunction3<Object, Vdata, HashMap<Long, float[]>, Vdata> implements Serializable {

    /**
     * 更新点的 embedding 和 feat
     * @param vID 点 ID
     * @param v 点
     * @param embedding 二度子图 embedding
     * @return Vdata 更新 embedding map 和 feat 后的点
     */
    @Override
    public Vdata apply(Object vID, Vdata v, HashMap<Long, float[]> embedding) {
        Vdata vdata = new Vdata((Long) vID, v);
        // 防止 src 的 embedding 被 init msg 置空
        if (v.getEmbedding().size() == 0) {
            vdata.setEmbedding(embedding);
        }
        // 防止 embedding map 里没有某点的 embedding 将 feat 篡改为 null
        if (embedding.containsKey(vID)) {
            vdata.setFeat(embedding.get(vID));
        }
        return vdata;
    }
}
