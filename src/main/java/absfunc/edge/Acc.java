package absfunc.edge;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.training.evaluator.Accuracy;
import dataset.Edata;
import org.apache.spark.graphx.Edge;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;

/**
 * @author napdada
 * @version : v 0.1 2021/12/8 11:08
 */
public class Acc extends AbstractFunction1<Edge<Edata>, Integer> implements Serializable {
    @Override
    public Integer apply(Edge<Edata> e) {
        try(NDManager manager = NDManager.newBaseManager()) {
            NDArray logits = manager.create(e.attr().getLogits());
            NDArray labels = manager.create(e.attr().getLabels());
            Accuracy accuracy = new Accuracy();
            long[] res = accuracy.evaluate(new NDList(labels), new NDList(logits)).toLongArray();
            return (int) res[0];
        }
    }
}
