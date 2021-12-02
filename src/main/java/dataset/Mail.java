package dataset;

import config.Constants;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Arrays;

/**
 * 点 Mail
 *
 * @author napdada
 * @version : v 0.1 2021/11/3 10:54 上午
 */
@Getter
@Setter
public class Mail implements Serializable {
    /**
     * mail 个数
     */
    private int num;
    /**
     * mail 特征维度
     */
    private int dim;
    /**
     * mail 特征
     */
    private float[] feat;

    public Mail() {
        this.num = 1;
        initDim();
        this.feat = new float[dim];
    }

    public Mail(float[] feat) {
        this.num = 1;
        initDim();
        this.feat = feat;
    }

    public Mail(int num, float[] feat) {
        this.num = num;
        this.feat = feat;
    }

    /**
     * 初始化 mail 特征的维度以及是否使用 time embedding 和 position embedding
     */
    public final void initDim() {
        dim = Constants.FEATURE_DIM;
        if (Constants.TIME_EMBEDDING) {
            dim++;
        }
        if (Constants.POSITION_EMBEDDING) {
            dim++;
        }
    }

    @Override
    public String toString() {
        return "Mail{" +
                "num=" + num +
                ", dim=" + dim +
                ", feat=" + Arrays.toString(feat) +
                '}';
    }
}
