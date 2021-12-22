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

    private float timestamp;

    public FilterByTs(float timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public Object apply(Edge<Edata> e) {
        return e.attr().getTimeStamp() == timestamp && e.attr().getAccuracy() == 0;
    }
}
