package absfunc.triplet;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import dataset.Edata;
import dataset.Mail;
import dataset.Vdata;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.graphx.EdgeContext;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;

/**
 * 沿着每条边给 dst 发送 feat msg
 *
 * @note NDArray 不可被序列化，NDArray 的基本运算结果需要持久化到 Java 对象中（eg. Feat.class）
 * @author napdada
 * @version : v 0.1 2021/11/16 9:01 下午
 */
@Getter
@Setter
public class SendMail extends AbstractFunction1<EdgeContext<Vdata, Edata, Mail>, BoxedUnit> implements Serializable {
    /**
     * 发送 feat msg = (e.src['feat] + e.dst['feat'] + e['feat']).concat(timestamp).concat(location)
     * @param e 边
     * @return BoxedUnit.UNIT
     */
    @Override
    public BoxedUnit apply(EdgeContext<Vdata, Edata, Mail> e) {
        try(NDManager manager = NDManager.newBaseManager()) {
            HashMap<Long, float[]> srcMap = e.srcAttr().getEmbedding();
            HashMap<Long, float[]> dstMap = e.dstAttr().getEmbedding();
            if (srcMap.size() != 0 && dstMap.size() != 0) {
                NDArray src = manager.create(e.srcAttr().getFeat());
                NDArray dst = manager.create(e.dstAttr().getFeat());
                NDArray edge = manager.create(e.attr().getFeat());
                NDArray featMsg = src.add(dst).add(edge);
                NDArray edgeTimestamp = manager.create(new float[] {e.attr().getTimeStamp()});
                NDArray location = manager.create(new float[] {1f});
                e.sendToSrc(new Mail(featMsg.concat(edgeTimestamp).concat(location).toFloatArray()));
                e.sendToDst(new Mail(featMsg.concat(edgeTimestamp).concat(location).toFloatArray()));
            }
        }
        return BoxedUnit.UNIT;
    }
}
