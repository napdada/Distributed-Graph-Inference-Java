package absfunc.edge;

import org.apache.spark.graphx.Edge;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;

/**
 * 过滤出判断错误的事件（accuracy == 0）
 * @author napdada
 * @version : v 0.1 2021/12/8 17:31
 */
public class NegEdge extends AbstractFunction1<Edge<Integer>, Object> implements Serializable {
    @Override
    public Object apply(Edge<Integer> e) {
        return e.attr() == 0;
    }
}
