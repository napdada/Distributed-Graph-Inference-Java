package absfunc.triplet;

import dataset.Edata;
import dataset.Vdata;
import org.apache.spark.graphx.EdgeTriplet;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.Collections;

/**
 * 沿着每条边给 dst 发送边的 timestamp
 *
 * @author napdada
 * @version : v 0.1 2021/11/10 7:35 下午
 */
public class SendTimestamp extends AbstractFunction1<EdgeTriplet<Vdata, Edata>, Iterator<Tuple2<Object, Float>>> implements Serializable {
    /**
     * 沿着每条边给 dst 节点发送边的 timestamp
     * @param e 边
     * @return BoxedUnit.UNIT
     */
    @Override
    public Iterator<Tuple2<Object, Float>> apply(EdgeTriplet<Vdata, Edata> e) {
        return JavaConverters.asScalaIterator(Collections.singletonList(new Tuple2<Object, Float>(e.dstId(), e.attr().getTimeStamp())).iterator());
    }
}
