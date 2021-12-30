package absfunc.triplet;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.training.evaluator.Accuracy;
import config.Constants;
import dataset.Edata;
import dataset.Vdata;
import lombok.Getter;
import lombok.Setter;
import model.Decoder;
import model.DecoderInput;
import model.DecoderOutput;
import org.apache.spark.graphx.EdgeTriplet;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;

/**
 * 将 Decoder 模型的结果（logits、labels 更新到边上）
 *
 * @author napdada
 * @version : v 0.1 2021/12/7 20:35
 */
@Getter
@Setter
public class UpdateTriplet extends AbstractFunction1<EdgeTriplet<Vdata, Edata>, Edata> implements Serializable {
    /**
     * src ID
     */
    private Long src;
    /**
     * dst ID
     */
    private Long dst;

    public UpdateTriplet(Long src, Long dst) {
        this.src = src;
        this.dst = dst;
    }

    /**
     * 根据三类不同的任务（LP、NC、EC），处理边上的数据为 Decoder 模型的输入
     * @param e EdgeTriplet<Vdata, Edata>
     * @return Edata
     */
    @Override
    public Edata apply(EdgeTriplet<Vdata, Edata> e) {
        if ((e.srcId() == src && e.dstId() == dst) || (e.srcId() == dst && e.dstId() == src)) {
            Decoder decoder = Decoder.getInstance();
            try(NDManager manager = NDManager.newBaseManager()) {
                NDArray src = manager.create(e.srcAttr().getFeat());
                NDArray dst = manager.create(e.dstAttr().getFeat());
                NDArray neg = manager.create(new float[Constants.FEATURE_DIM]);
                float[][] posEmb = new float[1][], negEmb = {src.concat(neg).toFloatArray()};
                float[] posLabel = {e.attr().getLabel()}, negLabel = {0f};

                switch (Constants.TASK_NAME) {
                    case "LP":
                        posEmb[0] = src.concat(dst).toFloatArray();
                        break;
                    case "EC":
                        NDArray eFeat = manager.create(e.attr().getFeat());
                        posEmb[0] = src.concat(dst).concat(eFeat).toFloatArray();
                        break;
                    case "NC":
                        posEmb[0] = src.toFloatArray();
                        break;
                    default:
                        System.out.println("参数 TASK_NAME 配置错误！");
                        return e.attr();
                }
                DecoderInput decoderInput = new DecoderInput(posEmb, posLabel, negEmb, negLabel);
                DecoderOutput decoderOutput = decoder.infer(decoderInput);

                NDArray logits = manager.create(decoderOutput.getLogic());
                NDArray labels = manager.create(decoderOutput.getLabel());
                Accuracy accuracy = new Accuracy();
                long[] acc = accuracy.evaluate(new NDList(labels), new NDList(logits)).toLongArray();
                return new Edata(e.attr(), decoderOutput, (int) acc[0]);
            }
        }
        return e.attr();
    }
}
