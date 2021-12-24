package absfunc.vertex;

import dataset.Vdata;
import lombok.Getter;
import lombok.Setter;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;

/**
 * @author napdada
 * @version : v 0.1 2021/12/23 10:55
 */
public class UpdateHop extends AbstractFunction3<Object, Vdata, Integer, Vdata> implements Serializable {

    @Override
    public Vdata apply(Object vID, Vdata v, Integer hop) {
        Vdata newV = new Vdata();
        if (v.getHop() == 2 || v.getHop() < hop) {
            newV.setHop(hop);
        } else {
            newV.setHop(v.getHop());
        }
        return newV;
    }
}
