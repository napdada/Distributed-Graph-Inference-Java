package dataset;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Arrays;

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
     * 边特征维度
     */
    private int featDim;
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

    public Edata() {

    }

    public Edata(int featDim, float[] feat, int label, float timeStamp) {
        this.featDim = featDim;
        this.feat = feat;
        this.label = label;
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "Edata{" +
                "featDim=" + featDim +
                ", feat=" + Arrays.toString(feat) +
                ", label=" + label +
                ", timeStamp=" + timeStamp +
                '}';
    }
}
