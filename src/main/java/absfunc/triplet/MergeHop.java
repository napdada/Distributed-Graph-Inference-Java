package absfunc.triplet;

import scala.runtime.AbstractFunction2;

import java.io.Serializable;

/**
 * @author napdada
 * @version : v 0.1 2021/12/23 10:57
 */
public class MergeHop extends AbstractFunction2<Integer, Integer, Integer> implements Serializable {
    @Override
    public Integer apply(Integer hop1, Integer hop2) {
        return hop1 > hop2 ? hop1 : hop2;
    }
}
