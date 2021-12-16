package model;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * Pytorch Decoder 模型自定义输入
 *
 * @author napdada
 * @version : v 0.1 2021/12/7 18:58
 */
@Getter
@Setter
public class DecoderInput implements Serializable {
    /**
     * pos embedding
     */
    private float[][] posEmb;

    /**
     * neg embedding
     */
    private float[][] negEmb;

    /**
     * pos Label
     */
    private float[] posLabel;

    /**
     * neg Label
     */
    private float[] negLabel;

    public DecoderInput(float[][] posEmb, float[] posLabel, float[][] negEmb, float[] negLabel) {
        this.posEmb = posEmb;
        this.posLabel = posLabel;
        this.negEmb = negEmb;
        this.negLabel = negLabel;
    }
}
