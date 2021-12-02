package absfunc.triplet;

import scala.runtime.AbstractFunction2;

import java.io.Serializable;

/**
 * src 从收到的 timestamps 中选最大的作为点的 timestamp
 *
 * @author napdada
 * @version : v 0.1 2021/11/10 7:35 下午
 */
public class MergeTimestamp extends AbstractFunction2<Float, Float, Float> implements Serializable {
    /**
     * 比较点收到的 timestamps 挑出最大的
     * @param timestamp1 时间戳 1
     * @param timestamp2 时间戳 2
     * @return 最大 timestamp
     */
    @Override
    public Float apply(Float timestamp1, Float timestamp2) {
        return timestamp1 > timestamp2 ? timestamp1 : timestamp2;
    }
}
