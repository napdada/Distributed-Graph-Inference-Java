package absfunc.triplet;

import dataset.Edata;
import dataset.Vdata;
import dataset.Vfeat;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.graphx.EdgeContext;
import org.apache.spark.graphx.EdgeTriplet;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;

/**
 * @author napdada
 * @version : v 0.1 2022/1/20 21:58
 */
@Getter
@Setter
public class SendV extends AbstractFunction1<EdgeTriplet<Vdata, Edata>,
        Iterator<Tuple2<Object, HashMap<Long, Vfeat>>>> implements Serializable {
    /**
     * src ID
     */
    private Long src;
    /**
     * dst ID
     */
    private Long dst;

    public SendV(Long src, Long dst) {
        this.src = src;
        this.dst = dst;
    }

    @Override
    public Iterator<Tuple2<Object, HashMap<Long, Vfeat>>> apply(EdgeTriplet<Vdata, Edata> e) {
        if (e.srcId() == src && e.dstId() == dst || e.srcId() == dst && e.dstId() == src) {
            return JavaConverters.asScalaIterator(Collections.singletonList(
                    new Tuple2<Object, HashMap<Long, Vfeat>>(e.dstId(), e.srcAttr().getEventSubgraph2DFeat()))
                    .iterator());
        }
        return JavaConverters.asScalaIterator(Collections.emptyIterator());
    }

}
