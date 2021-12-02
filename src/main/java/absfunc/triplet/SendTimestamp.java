package absfunc.triplet;

import dataset.Edata;
import dataset.Vdata;
import org.apache.spark.graphx.EdgeContext;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import java.io.Serializable;

/**
 * 沿着每条边给 dst 发送边的 timestamp
 *
 * @author napdada
 * @version : v 0.1 2021/11/10 7:35 下午
 */
public class SendTimestamp extends AbstractFunction1<EdgeContext<Vdata, Edata, Float>, BoxedUnit> implements Serializable {
    /**
     * 沿着每条边给 dst 节点发送边的 timestamp
     * @param e 边
     * @return BoxedUnit.UNIT
     */
    @Override
    public BoxedUnit apply(EdgeContext<Vdata, Edata, Float> e) {
        e.sendToDst(e.attr().getTimeStamp());
        return BoxedUnit.UNIT;
    }
}
