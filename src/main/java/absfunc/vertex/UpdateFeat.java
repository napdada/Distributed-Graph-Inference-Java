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
            // TODO: 可以优化成 v.setEmbedding()，但是 set 后后续的 send 里 srcAttr 依赖没变（BUG，不知道原因）
            Vdata vdata = new Vdata((Long) vID, v, embedding);
            vdata.setFeat(embedding.get((Long) vID));
            return vdata;
        }
        return v;
    }

}
