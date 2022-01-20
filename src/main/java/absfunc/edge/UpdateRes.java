package absfunc.edge;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.training.evaluator.Accuracy;
import dataset.Edata;
import dataset.Vdata;
import lombok.Getter;
import lombok.Setter;
import model.Decoder;
import model.DecoderInput;
import model.DecoderOutput;
import org.apache.spark.graphx.EdgeTriplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.Arrays;

import static config.Constants.*;

/**
 * 将 Decoder 模型的结果（logits、labels、accuracy 更新到边上）
 *
 * @author napdada
 * @version : v 0.1 2021/12/7 20:35
 */
@Getter
@Setter
public class UpdateRes extends AbstractFunction1<EdgeTriplet<Vdata, Edata>, Edata> implements Serializable {
    /**
     * Log
     */
    private static final Logger logger = LoggerFactory.getLogger(UpdateRes.class);
    /**
     * src ID
     */
    private Long src;
    /**
     * dst ID
     */
    private Long dst;

    public UpdateRes(Long src, Long dst) {
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
                NDArray neg = manager.create(new float[FEATURE_DIM]);
                float[][] posEmb = new float[1][], negEmb = {src.concat(neg).toFloatArray()};
                float[] posLabel = {e.attr().getLabel()}, negLabel = {0f};

                switch (TASK_NAME) {
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
                        logger.error("UpdateRes apply(): 参数 TASK_NAME 配置错误！");
                        return e.attr();
                }
                DecoderInput decoderInput = new DecoderInput(posEmb, posLabel, negEmb, negLabel);
                DecoderOutput decoderOutput = decoder.infer(decoderInput);
                float[] logit = decoderOutput.getLogic();
                if (TASK_NAME.equals("LP")) {
                    logit[0] = logit[0] > 0.5 ? 1 : 0;
                    logit[1] = logit[1] > 0.5 ? 1 : 0;
                }

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
