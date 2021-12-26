package absfunc.triplet;

import dataset.Edata;
import dataset.Vdata;
import dataset.Vfeat;
import org.apache.spark.graphx.EdgeTriplet;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;

/**
 * GraphOps.pregel 形参中 sendMsg 的实现
 * 每轮迭代发送 src 的 event subgraph2D feat map 给 dst
 * @author napdada
 * @version : v 0.1 2021/12/24 21:35
 */
public class SendVfeat extends AbstractFunction1<EdgeTriplet<Vdata, Edata>,
        Iterator<Tuple2<Object, HashMap<Long, Vfeat>>>> implements Serializable {
    /**
     * 二度子图的 vfeat 收集
     * 1. 第一轮 hop = 0 给 hop = 1 发（二度点给一度点发）
     * 2. 第二轮 hop = 1 给 hop = 1 发（一度点给 src、dst 发）
     * 3. 第三轮 src、dst 互发，将 2DSubgraph feat 汇总到 src、dst
     * @param e EdgeTriplet<Vdata, Edata>
     * @return Iterator<Tuple2<Object, HashMap<Long, Vfeat>>> 给 src 发送的 event subgraph2D feat map
     */
    @Override
    public Iterator<Tuple2<Object, HashMap<Long, Vfeat>>> apply(EdgeTriplet<Vdata, Edata> e) {
        if (e.srcAttr().getHop() == 0 && e.dstAttr().getHop() == 1) {
            return JavaConverters.asScalaIterator(Collections.singletonList(
                    new Tuple2<Object, HashMap<Long, Vfeat>>(e.dstId(), e.srcAttr().getEventSubgraph2DFeat()))
                    .iterator());
        } else if (e.srcAttr().getHop() == 1 && e.dstAttr().getHop() == 2) {
            return JavaConverters.asScalaIterator(Collections.singletonList(
                    new Tuple2<Object, HashMap<Long, Vfeat>>(e.dstId(), e.srcAttr().getEventSubgraph2DFeat()))
                    .iterator());
        } else if (e.srcAttr().getHop() == 2 && e.dstAttr().getHop() == 2) {
            return JavaConverters.asScalaIterator(Collections.singletonList(
                    new Tuple2<Object, HashMap<Long, Vfeat>>(e.dstId(), e.srcAttr().getEventSubgraph2DFeat()))
                    .iterator());
        } else {
            return JavaConverters.asScalaIterator(Collections.emptyIterator());
        }
    }
}
