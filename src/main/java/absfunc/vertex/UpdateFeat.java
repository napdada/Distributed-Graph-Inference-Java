package absfunc.vertex;

import dataset.Vdata;
import dataset.Vfeat;
import lombok.Getter;
import lombok.Setter;
import model.EncoderInput;
import model.EncoderOutput;
import model.Encoder;
import scala.runtime.AbstractFunction2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Graph.mapVertices 参数中 map 的实现
 * 将 encoder 得到的 embedding 更新到 Vdata feat 上
 * @author napdada
 * @version : v 0.1 2021/11/29 7:45 下午
 */
@Getter
@Setter
public class UpdateFeat extends AbstractFunction2<Object, Vdata, Vdata> implements Serializable {
    /**
     * 点 ID
     */
    private Long src;

    public UpdateFeat(Long src) {
        this.src = src;
    }

    /**
     * 1. 将二度子图（2DSubgraph）转成 encoder 的输入格式
     * 2. 加载 encoder.pt 模型进行推理，得到 embedding output
     * 3. 将 embedding output 更新到 Vdata feat 上
     * @param vID 点 ID
     * @param v 点属性
     * @return 更新后的 Vdata
     */
    @Override
    public Vdata apply(Object vID, Vdata v) {
        if (vID.equals(src)) {
            HashMap<Long, float[]> embedding = new HashMap<>();
            HashMap<Long, Vfeat> map = v.getEventSubgraph2DFeat();
            int num = map.size(), i = 0;
            long[] index = new long[num];
            float[][] feat = new float[num][];
            float[][][] mail = new float[num][][];
            float[] lastUpdate = new float[num], timestamp = new float[num];

            for (Map.Entry<Long, Vfeat> entry : map.entrySet()) {
                index[i] = entry.getKey();
                feat[i] = entry.getValue().getFeat();
                mail[i] = entry.getValue().mailboxToArray();
                lastUpdate[i] = entry.getValue().getLastUpdate();
                timestamp[i] = entry.getValue().getTimestamp();
                i++;
            }
            Encoder encoder = Encoder.getInstance();
            EncoderInput encoderInput = new EncoderInput(feat, mail, lastUpdate, timestamp);
            EncoderOutput encoderOutput = encoder.infer(encoderInput);

            for (int j = 0; j < index.length; j++) {
                embedding.put(index[j], encoderOutput.getEmbedding()[j]);
            }
            Vdata vdata = new Vdata((Long) vID, v);
            vdata.setEmbedding(embedding);
            vdata.setFeat(embedding.get((Long) vID));
            return vdata;
        }
        return v;
    }

}
