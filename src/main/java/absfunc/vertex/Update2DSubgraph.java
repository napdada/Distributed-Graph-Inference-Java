package absfunc.vertex;

import dataset.Vdata;
import dataset.Vfeat;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.HashMap;

/**
 * GraphOps.pregel 形参中 vprog 的实现
 * 将 pregel 每轮迭代 event subgraph2D feat map msg 更新到点上
 * @author napdada
 * @version : v 0.1 2021/12/24 21:29
 */
public class Update2DSubgraph extends AbstractFunction3<Object, Vdata, HashMap<Long, Vfeat>, Vdata> implements Serializable {
    /**
     * 更新点的 event subgraph2D feat map
     * @param vID 点 ID
     * @param v 点
     * @param eventSubgraph 新事件的二度子图 map
     * @return Vdata 更新 eventSubgraph 后的点
     */
    @Override
    public Vdata apply(Object vID, Vdata v, HashMap<Long, Vfeat> eventSubgraph) {
        HashMap<Long, Vfeat> map = new HashMap<>();
        map.putAll(v.getEventSubgraph2DFeat());
        map.putAll(eventSubgraph);
        map.put((Long) vID, new Vfeat(v));
        return new Vdata((Long) vID, v, map);
    }
}
