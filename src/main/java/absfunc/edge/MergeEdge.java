package absfunc.edge;

import dataset.Edata;
import scala.runtime.AbstractFunction2;

import java.io.Serializable;

/**
 * GraphX.groupEdges() 参数中 merge 的实现，对于同 src、dst 边选最新的边
 *
 * @author napdada
 * @version : v 0.1 2021/11/8 4:17 下午
 */
public class MergeEdge extends AbstractFunction2<Edata, Edata, Edata> implements Serializable {
    /**
     * 比较同 src、dst 的多条边，选取最新的边
     * @param e1 边 1
     * @param e2 边 2
     * @return 时间戳最大的边
     */
    @Override
    public Edata apply(Edata e1, Edata e2) {
        return e1.getTimeStamp() < e2.getTimeStamp() ? e2 : e1;
    }
}
