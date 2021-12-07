package model;

import dataset.Vdata;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Pytorch Encoder 模型自定义输出
 *
 * @author napdada
 * @version : v 0.1 2021/11/15 5:35 下午
 */
@Getter
@Setter
public class EncoderOutput implements Serializable {
    /**
     * Log
     */
    private static final Logger logger = LoggerFactory.getLogger(EncoderOutput.class);
    /**
     * 模型输出 embedding
     */
    private float[][] embedding;

    public EncoderOutput() {

    }

    public EncoderOutput(float[][] embedding) {
        this.embedding = embedding;
    }

    @Override
    public String toString() {
        return "EncoderOutput{" +
                "embedding=" + Arrays.toString(embedding) +
                '}';
    }

    /**
     * 将模型输出的 embedding 转换成点 JavaRDD（点 ID 和 点 embedding 对应）
     * @param vIndex 点 ID
     * @param sc JavaSparkContext
     * @return JavaRDD<Tuple2<Object, float[]>>
     */
    public JavaRDD<Tuple2<Object, Vdata>> toJavaRDD(Long[] vIndex, JavaSparkContext sc) {
        ArrayList<Tuple2<Object, Vdata>> vList = new ArrayList<>();
        for (int i = 0; i < vIndex.length; i++) {
            Vdata vdata = new Vdata();
            vdata.setFeat(embedding[i]);
            vList.add(new Tuple2<>(vIndex[i], vdata));
        }
        return sc.parallelize(vList);
    }
}
