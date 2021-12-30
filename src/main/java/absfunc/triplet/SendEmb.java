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
import java.util.HashMap;

/**
 * GraphOps.pregel 参数中 sendMsg 的实现
 * 将 src 上的推理结果 embedding 发给二度子图中所有点
 * @author napdada
 * @version : v 0.1 2021/12/25 11:14
 */
@Getter
@Setter
public class SendEmb extends AbstractFunction1<EdgeTriplet<Vdata, Edata>,
        Iterator<Tuple2<Object, HashMap<Long, float[]>>>> implements Serializable {
    /**
     * src ID
     */
    private Long src;
    /**
     * dst ID
     */
    private Long dst;

    public SendEmb(Long src, Long dst) {
        this.src = src;
        this.dst = dst;
    }

    /**
     * 将 src 推理得到的 embedding 结果发给二度子图中的所有点
     * 1. 第一轮，src 发给 dst
     * 2. 第二轮，src、dst 发给其一度点
     * 3. 第三轮，一度点发给二度点
     * @param e EdgeTriplet<Vdata, Edata>
     * @return Iterator<Tuple2<Object, HashMap<Long, float[]>>>
     */
    @Override
    public Iterator<Tuple2<Object, HashMap<Long, float[]>>> apply(EdgeTriplet<Vdata, Edata> e) {
        if (e.srcId() == src && e.dstId() == dst || e.dstId() == src && e.srcId() == dst) {
            return JavaConverters.asScalaIterator(Collections.singletonList(
                    new Tuple2<Object, HashMap<Long, float[]>>(e.dstId(), e.srcAttr().getEmbedding()))
                    .iterator());
        } else if (e.srcId() == src || e.srcId() == dst) {
            return JavaConverters.asScalaIterator(Collections.singletonList(
                    new Tuple2<Object, HashMap<Long, float[]>>(e.dstId(), e.srcAttr().getEmbedding()))
                    .iterator());
        } else if (e.srcAttr().getHop() == 1 && e.dstAttr().getHop() == 0) {
            return JavaConverters.asScalaIterator(Collections.singletonList(
                    new Tuple2<Object, HashMap<Long, float[]>>(e.dstId(), e.srcAttr().getEmbedding()))
                    .iterator());
        } else {
            return JavaConverters.asScalaIterator(Collections.emptyIterator());
        }
    }
}
