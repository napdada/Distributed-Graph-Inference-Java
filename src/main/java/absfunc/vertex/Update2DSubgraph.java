package absfunc.vertex;

import config.Constants;
import dataset.Vdata;
import dataset.Vfeat;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.HashMap;

/**
 * @author napdada
 * @version : v 0.1 2021/12/24 21:29
 */
public class Update2DSubgraph extends AbstractFunction3<Object, Vdata, HashMap<Long, Vfeat>, Vdata> implements Serializable {
    @Override
    public Vdata apply(Object vID, Vdata v, HashMap<Long, Vfeat> eventSubgraph) {
        HashMap<Long, Vfeat> map = new HashMap<>();
        map.putAll(v.getEventSubgraph2DFeat());
        map.putAll(eventSubgraph);
        map.put((Long) vID, new Vfeat(v));
        return new Vdata((Long) vID, v, map);
    }
}
