package absfunc.vertex;

import dataset.Vdata;
import lombok.Getter;
import lombok.Setter;
import scala.Tuple2;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;

/**
 * @author napdada
 * @version : v 0.1 2021/11/30 7:32 下午
 */
@Getter
@Setter
public class OneVertex extends AbstractFunction1<Tuple2<Object, Vdata>, Object> implements Serializable {

    private Long id;

    public OneVertex(Long id) {
        this.id = id;
    }

    @Override
    public Object apply(Tuple2<Object, Vdata> v) {
        return v._1.equals(id);
    }
}
