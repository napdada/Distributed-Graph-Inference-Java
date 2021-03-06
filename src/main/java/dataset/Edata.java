package dataset;

import lombok.Getter;
import lombok.Setter;
import model.DecoderOutput;

import java.io.Serializable;
import java.util.Arrays;

import static config.Constants.*;

/**
 * 边属性定义
 *
 * @author napdada
 * @version : v 0.1 2021/11/2 5:39 下午
 */
@Getter
@Setter
public class Edata implements Serializable {
    /**
     * 边特征
     */
    private float[] feat;
    /**
     * 边标签
     */
    private int label;
    /**
     * 边时间戳
     */
    private float timeStamp;
    /**
     * MLP Decoder 推理结果
     */
    private float[] logits;
    /**
     * MLP Decoder 标签
     */
    private float[] labels;
    /**
     * Decoder accuracy
     */
    private int accuracy;

    public Edata() {
        this.feat = new float[FEATURE_DIM];
        this.label = 0;
        this.timeStamp = 0;
    }

    public Edata(float[] feat, int label, float timeStamp) {
        this.feat = feat;
        this.label = label;
        this.timeStamp = timeStamp;
    }

    public Edata(Edata e, DecoderOutput decoderOutput, int accuracy) {
        this.feat = e.getFeat();
        this.label = e.getLabel();
        this.timeStamp = e.getTimeStamp();
        this.logits = decoderOutput.getLogic();
        this.labels = decoderOutput.getLabel();
        this.accuracy = accuracy;
    }

    @Override
    public String toString() {
        return "Edata{" +
                "feat=" + Arrays.toString(feat) +
                ", label=" + label +
                ", timeStamp=" + timeStamp +
                ", logits=" + Arrays.toString(logits) +
                ", labels=" + Arrays.toString(labels) +
                ", accuracy=" + accuracy +
                '}';
    }
}
