package absfunc.vertex;

import dataset.Vdata;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;

/**
 * GraphX.outerJoinVertices() 形参中 mapFunc 的实现，更新部分点属性中的 lastUpdate 或 timestamp
 *
 * @author napdada
 * @version : v 0.1 2021/11/16 2:13 下午
 */
@Getter
@Setter
public class UpdateTime extends AbstractFunction3<Object, Vdata, Float, Vdata> implements Serializable {
    /**
     * Log
     */
    private static final Logger logger = LoggerFactory.getLogger(UpdateTime.class);

    /**
     * 更新点的 lastUpdate 或 timestamp
     * @note 【！！！不能直接用 v.setTimestamp() 否则所有点的 timestamp 都被改了，需要 new Vdata()，原因未知！！！】
     * @param vID 点 ID
     * @param v 待更新的点
     * @param newV 新的 time
     * @return 更新后的点
     */
    @Override
    public Vdata apply(Object vID, Vdata v, Float timestamp) {
        return new Vdata((Long) vID, v.getFeat(), v.getMailbox(), timestamp, timestamp);
    }
}
