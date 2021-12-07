package model;

import config.Constants;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.ArrayUtils;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Pytorch Encoder 模型自定义输入
 *
 * @author napdada
 * @version : v 0.1 2021/11/15 4:27 下午
 */
@Getter
@Setter
public class EncoderInput implements Serializable {
    /**
     * 点特征
     */
    private float[][] feat;
    /**
     * 点邮箱
     */
    private float[][][] mail;
    /**
     * 点最后更新时间
     */
    private float[] lastUpdate;
    /**
     * 点时间戳
     */
    private float[] timestamp;
    /**
     * 点边特征维度
     */
    private int featDim;
    /**
     * mail 特征维度
     */
    private int mailDim;

    public EncoderInput() {

    }

    public EncoderInput(float[][] feat, float[][][] mail, float[] lastUpdate, float[] timestamp) {
        this.featDim = Constants.FEATURE_DIM;
        this.mailDim = Constants.FEATURE_DIM;
        if (Constants.TIME_EMBEDDING) {
            mailDim++;
        }
        if (Constants.POSITION_EMBEDDING) {
            mailDim++;
        }
        this.feat = feat;
        this.mail = mail;
        this.lastUpdate = lastUpdate;
        this.timestamp = timestamp;
    }

    /**
     * 将三维数组 mail 降成一维
     * @return float[]
     */
    public float[] flatMail() {
        float[] res = new float[0];
        for (int x = 0; x < mail.length; x++) {
            for (int y = 0; y < mail[x].length; y++) {
                res = ArrayUtils.addAll(res, mail[x][y]);
            }
        }
        return res;
    }

    @Override
    public String toString() {
        return "EncoderInput{" +
                "feat=" + Arrays.toString(feat) +
                ", mail=" + Arrays.toString(mail) +
                ", lastUpdate=" + Arrays.toString(lastUpdate) +
                ", timestamp=" + Arrays.toString(timestamp) +
                '}';
    }
}
