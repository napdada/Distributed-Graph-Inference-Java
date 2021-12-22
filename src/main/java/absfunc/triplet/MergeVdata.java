package absfunc.triplet;

import dataset.Vdata;
import lombok.Getter;
import lombok.Setter;
import scala.runtime.AbstractFunction2;

import java.io.Serializable;

/**
 * @author napdada
 * @version : v 0.1 2021/11/30 5:09 下午
 */
@Getter
@Setter
public class MergeVdata  extends AbstractFunction2<Vdata, Vdata, Vdata> implements Serializable {

    public int turnNum;

    public String mergeName;

    public MergeVdata(String mergeName) {
        this.mergeName = mergeName;
    }

    public MergeVdata(int turnNum, String mergeName) {
        this.turnNum = turnNum;
        this.mergeName = mergeName;
    }

    @Override
    public Vdata apply(Vdata v1, Vdata v2) {
        switch (mergeName) {
            case "vertex":
                v1.getSubgraph2D().addAll(v2.getSubgraph2D());
                v1.getSubgraph2DFeat().putAll(v2.getSubgraph2DFeat());
                break;
            case "event":
                if (turnNum == 0) {
                    v1.getSubgraph2D().addAll(v2.getSubgraph2D());
                    v1.getSubgraph2DFeat().putAll(v2.getSubgraph2DFeat());
                } else {
                    v1.getEventSubgraph2D().addAll(v2.getEventSubgraph2D());
                }
                break;
            case "embedding":
                v1.getEmbedding().putAll(v2.getEmbedding());
                break;
        }
        return v1;
    }
}
