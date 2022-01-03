package absfunc.edge;

import dataset.Edata;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.graphx.Edge;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;

/**
 * 过滤出最新发生事件 (src, dst) 中 accuracy 为正确/错误的边
 * @author napdada
 * @version : v 0.1 2021/12/16 20:33
 */
@Getter
@Setter
public class NegFilter extends AbstractFunction1<Edge<Edata>, Object> implements Serializable {
    /**
     * src ID
     */
    private Long src;
    /**
     * dst ID
     */
    private Long dst;

    public NegFilter(Long src, Long dst) {
        this.src = src;
        this.dst = dst;
    }

    @Override
    public Object apply(Edge<Edata> e) {
        return ((e.srcId() == src && e.dstId() == dst) || (e.srcId() == dst && e.dstId() == src)) && e.attr().getAccuracy() == 0;
    }
}
