package absfunc.vertex;

import dataset.Vdata;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction2;

import java.io.Serializable;

/**
 * GraphX.outerJoinVertices() 参数中 mapFunc 的实现
 * 更新部分点属性中的 lastUpdate 或 timestamp
 * @author napdada
 * @version : v 0.1 2021/11/16 2:13 下午
 */
@Getter
@Setter
public class UpdateTime extends AbstractFunction2<Object, Vdata, Vdata> implements Serializable {
    /**
     * Log
     */
    private static final Logger logger = LoggerFactory.getLogger(UpdateTime.class);

    /**
     * src ID
     */
    private Long src;
    /**
     * dst ID
     */
    private Long dst;
    /**
     * (src, dst) 最新事件时间戳
     */
    private float timestamp;

    public UpdateTime(Long src, Long dst, float timestamp) {
        this.src = src;
        this.dst = dst;
        this.timestamp = timestamp;
    }
    /**
     * 更新点的 lastUpdate 和 timestamp
     * @param vID 点 ID
     * @param v 待更新的点
     * @return 更新后的点
     */
    @Override
    public Vdata apply(Object vID, Vdata v) {
        if (vID.equals(src) || vID.equals(dst)) {
            if (v.getMailbox().size() != 0) {
                return new Vdata((Long) vID, v.getFeat(), v.getMailbox(), timestamp, timestamp);
            } else {
                return new Vdata((Long) vID, timestamp, timestamp);
            }
        }
        return v;
    }
}
