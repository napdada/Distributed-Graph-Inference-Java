package absfunc.edge;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.training.evaluator.Accuracy;
import dataset.Edata;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.graphx.Edge;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;

/**
 * @author napdada
 * @version : v 0.1 2021/12/16 20:22
 */
@Getter
@Setter
public class UpdateAcc extends AbstractFunction1<Edge<Edata>, Edata> implements Serializable {

    private float timestamp;

    public UpdateAcc(float timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public Edata apply(Edge<Edata> e) {
        if (e.attr().getTimeStamp() == timestamp) {
            try(NDManager manager = NDManager.newBaseManager()) {
                NDArray logits = manager.create(e.attr().getLogits());
                NDArray labels = manager.create(e.attr().getLabels());
                Accuracy accuracy = new Accuracy();
                long[] res = accuracy.evaluate(new NDList(labels), new NDList(logits)).toLongArray();
                return new Edata(e.attr(), (int) res[0]);
            }
        }
        return e.attr();
    }
}
