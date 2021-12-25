package absfunc.triplet;

import dataset.Vfeat;
import scala.runtime.AbstractFunction2;

import java.io.Serializable;
import java.util.HashMap;

/**
 * @author napdada
 * @version : v 0.1 2021/12/24 21:43
 */
public class MergeVfeat extends AbstractFunction2<HashMap<Long, Vfeat>, HashMap<Long, Vfeat>,
        HashMap<Long, Vfeat>> implements Serializable {
    @Override
    public HashMap<Long, Vfeat> apply(HashMap<Long, Vfeat> map1, HashMap<Long, Vfeat> map2) {
        map1.putAll(map2);
        return map1;
    }
}
