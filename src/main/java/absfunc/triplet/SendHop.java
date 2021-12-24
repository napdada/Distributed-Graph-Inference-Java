package absfunc.triplet;

import dataset.Edata;
import dataset.Vdata;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.graphx.EdgeTriplet;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.Collections;

/**
 * @author napdada
 * @version : v 0.1 2021/12/23 10:57
 */
@Getter
@Setter
public class SendHop extends AbstractFunction1<EdgeTriplet<Vdata, Edata>, Iterator<Tuple2<Object, Integer>>> implements Serializable {

    private Long src;
    private Long dst;

    public SendHop(Long src, Long dst) {
        this.src = src;
        this.dst = dst;
    }

    /**
     * 通过发消息的机制产生二度子图，每发一轮消息 hop 减 1
     * 1. 所有点 init hop = 2，src、dst 之间不发消息
     * 2. src、dst 分别给其一度点发消息（hop - 1）
     * 3. 一度点给二度点发消息（hop - 1）
     * 4. 其他的情况不发消息
     * 最后一度点 hop = 1，二度点 hop = 0，src、dst 和其他点 hop = 2
     * @param e EdgeTriplet
     * @return Iterator<Tuple2<Object, Integer>> 点 ID、点 hop
     */
    @Override
    public Iterator<Tuple2<Object, Integer>> apply(EdgeTriplet<Vdata, Edata> e) {
        // 1. src、dst 之间不发消息
        if (e.srcId() == src && e.dstId() == dst || e.dstId() == src && e.srcId() == dst) {
            return JavaConverters.asScalaIterator(Collections.emptyIterator());
        } else if (e.srcId() == src || e.srcId() == dst) {
            return JavaConverters.asScalaIterator(Collections.singletonList(new Tuple2<Object, Integer>(e.dstId(), e.srcAttr().getHop() - 1)).iterator());
        } else if (e.srcAttr().getHop() < 2 && e.dstId() != src && e.dstId() != dst) {
            return JavaConverters.asScalaIterator(Collections.singletonList(new Tuple2<Object, Integer>(e.dstId(), e.srcAttr().getHop() - 1)).iterator());
        } else {
            return JavaConverters.asScalaIterator(Collections.emptyIterator());
        }
    }
}
