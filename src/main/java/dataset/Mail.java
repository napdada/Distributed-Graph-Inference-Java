package dataset;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Arrays;

import static config.Constants.*;

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
        initDim();
        this.num = 1;
        this.feat = new float[dim];
    }

    public Mail(float[] feat) {
        initDim();
        this.num = 1;
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
        dim = FEATURE_DIM;
        if (TIME_EMBEDDING) {
            dim++;
        }
        if (POSITION_EMBEDDING) {
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
