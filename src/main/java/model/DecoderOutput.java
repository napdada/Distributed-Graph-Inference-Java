package model;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Pytorch Decoder 模型自定义输出
 *
 * @author napdada
 * @version : v 0.1 2021/12/7 18:58
 */
@Getter
@Setter
public class DecoderOutput implements Serializable {

    private float[] logic;

    private float[] label;

    public DecoderOutput(float[] logic, float[] label) {
        this.logic = logic;
        this.label = label;
    }

    @Override
    public String toString() {
        return "DecoderOutput{" +
                "logic=" + Arrays.toString(logic) +
                ", label=" + Arrays.toString(label) +
                '}';
    }
}
