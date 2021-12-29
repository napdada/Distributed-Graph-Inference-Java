package absfunc.edge;

import dataset.Edata;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.graphx.Edge;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;

/**
 * @author napdada
 * @version : v 0.1 2021/12/16 20:33
 */
@Getter
@Setter
public class FilterByTs extends AbstractFunction1<Edge<Edata>, Object> implements Serializable {

    private Long src;
    private Long dst;

    public FilterByTs(Long src, Long dst) {
        this.src = src;
        this.dst = dst;
    }

    @Override
    public Object apply(Edge<Edata> e) {
        boolean flag1 = e.srcId() == src && e.dstId() == dst;
        boolean flag2 = e.srcId() == dst && e.dstId() == src;
        return (flag1 || flag2) && e.attr().getAccuracy() != 0;
    }
}
