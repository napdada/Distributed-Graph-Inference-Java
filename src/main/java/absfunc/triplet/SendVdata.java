package absfunc.triplet;

import dataset.Edata;
import dataset.Vdata;
import dataset.Vfeat;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.graphx.EdgeContext;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;

/**
 * @author napdada
 * @version : v 0.1 2021/11/30 4:50 下午
 */
@Getter
@Setter
public class SendVdata extends AbstractFunction1<EdgeContext<Vdata, Edata, Vdata>, BoxedUnit> implements Serializable {
    private int turnNum;
    private String sendName;
    private Long src;
    private Long dst;

    public SendVdata(int turnNum, String sendName) {
        this.turnNum = turnNum;
        this.sendName = sendName;
    }

    public SendVdata(int turnNum, String sendName, Long src, Long dst) {
        this.turnNum = turnNum;
        this.sendName = sendName;
        this.src = src;
        this.dst = dst;
    }

    @Override
    public BoxedUnit apply(EdgeContext<Vdata, Edata, Vdata> e) {
        if (sendName.equals("vertex")) {
            if (turnNum == 1) {
                HashMap<Long, Vfeat> map = new HashMap<>();
                map.put(e.srcId(), new Vfeat(e.srcAttr()));
                e.sendToDst(new Vdata(null, map));
            } else if (turnNum == 2) {
                e.sendToDst(new Vdata(e.srcAttr().getSubgraph2D(), e.srcAttr().getSubgraph2DFeat()));
            }
        } else if (sendName.equals("event")) {
            if (turnNum == 0 && e.srcId() == src && e.dstId() == dst) {
                e.sendToDst(new Vdata(e.srcAttr().getSubgraph2D(), e.srcAttr().getSubgraph2DFeat()));
                e.sendToSrc(new Vdata(e.dstAttr().getSubgraph2D(), e.dstAttr().getSubgraph2DFeat()));
            } else if (turnNum == 1 && (e.srcId() == src || e.srcId() == dst)) {
                e.sendToDst(new Vdata(e.srcAttr().getEventSubgraph2D()));
            } else if (turnNum == 2) {
                String path = e.srcId() + "-" + e.dstId();
                HashSet<String> srcSet = e.srcAttr().getEventSubgraph2D();
                if (srcSet.contains(path)) {
                    e.sendToDst(new Vdata(srcSet));
                }
            }
        } else if (sendName.equals("embedding")) {
            if (turnNum == 0 && e.srcId() == src && e.dstId() == dst) {
                e.sendToDst(new Vdata(e.srcAttr().getEmbedding()));
            } else if (turnNum == 1 && (e.srcId() == src || e.srcId() == dst)) {
                e.sendToDst(new Vdata(e.srcAttr().getEmbedding()));
            } else if (turnNum == 2) {
                String path = e.srcId() + "-" + e.dstId();
                HashSet<String> srcSet = e.srcAttr().getEventSubgraph2D();
                if (srcSet.contains(path)) {
                    e.sendToDst(new Vdata(e.srcAttr().getEmbedding()));
                }
            }
        }
        return BoxedUnit.UNIT;
    }
}
