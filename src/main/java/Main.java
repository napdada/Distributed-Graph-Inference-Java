import config.Constants;
import dataset.GraphX;
import dataset.Edata;
import dataset.Vdata;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.io.*;
import java.text.SimpleDateFormat;

import static config.Constants.*;

/**
 * @author napdada
 * @version : v 0.1 2021/11/30 8:48 下午
 */
@Slf4j
public class Main {
    public static void main(String[] args) {
        try {
            // 1. Spark 初始化
            long sparkInitTime = System.currentTimeMillis(), sparkStartTime;
            Constants constants = new Constants(args);
            log.warn("--- " + constants.toString());
            JavaSparkContext sc = SC;
            sparkStartTime = System.currentTimeMillis();
            log.warn("--- Spark 初始化耗时：{} ms", sparkStartTime - sparkInitTime);

            // 2. 初始化数据集配置、图配置、模型配置
            BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(DATASET_PATH)));
            bufferedReader.readLine();                  // 去掉 csv 首行 title
            GraphX graphX = new GraphX();               // 图推理主类
            String lineData;                            // csv 中一行数据
            String[] line;                              // lineData 按 ',' 进行分隔
            long srcID, dstID;                          // 事件 (src, dst) 起始点、边目的点
            float timestamp;                            // 事件 (src, dst) 时间戳
            RDD<Tuple2<Object, Vdata>> vRDD = null;     // vertex RDD
            RDD<Edge<Edata>> eRDD = null;               // edge RDD
            int num = 0, count = 0;                     // num：事件数、count：evaluate 正确/错误数
            long initTime = sparkStartTime,
                    createGraphTime = 0, mergeTime = 0,
                    updateTsTime = 0, genNeighborTime = 0,
                    encoderTime = 0, updateMailboxTime = 0,
                    decoderTime = 0, evaluateTime = 0,
                    tmpTime;                            // 用于统计各步骤耗时
            log.warn("--- 初始化配置耗时：{} ms", System.currentTimeMillis() - initTime);

            // 3. 图推理迭代
            long startTime = System.currentTimeMillis();
            log.warn("--- {} 开始推理", new SimpleDateFormat(DATE_FORMAT).format(startTime));
            while ((lineData = bufferedReader.readLine()) != null) {
                // 3.1 读取一个新的事件，并与历史事件一起构图
                tmpTime = System.currentTimeMillis();
                line = lineData.split(",");
                srcID = Long.parseLong(line[SRC_ID_INDEX]);
                dstID = Long.parseLong(line[DST_ID_INDEX]);
                timestamp = Float.parseFloat(line[TIMESTAMP_INDEX]);
                if (eRDD == null) {
                    eRDD = sc.parallelize(graphX.eventToEdge(line)).rdd();
                } else {
                    eRDD = eRDD.union(sc.parallelize(graphX.eventToEdge(line)).rdd());
                }

                // 3.2 利用增量 vRDD、eRDD 构图
                graphX.creatGraph(vRDD, eRDD);
                createGraphTime += System.currentTimeMillis() - tmpTime;

                // 3.3 合并同 src、dst 边
                tmpTime = System.currentTimeMillis();
                graphX.mergeEdges();
                mergeTime += System.currentTimeMillis() - tmpTime;

                // 3.4 更新点时间戳
                tmpTime = System.currentTimeMillis();
                graphX.updateTimestamp(srcID, dstID, timestamp);
                updateTsTime += System.currentTimeMillis() - tmpTime;

                // 3.5 生成基于新事件的二度子图
                tmpTime = System.currentTimeMillis();
                graphX.event2DSubgraph(srcID, dstID);
                genNeighborTime += System.currentTimeMillis() - tmpTime;

                // 3.6 调用 encoder 进行图推理，并更新点 feat
                tmpTime = System.currentTimeMillis();
                graphX.encoder(srcID, dstID);
                encoderTime += System.currentTimeMillis() - tmpTime;

                // 3.7 更新点的 mailbox
                tmpTime = System.currentTimeMillis();
                graphX.updateMailbox();
                updateMailboxTime += System.currentTimeMillis() - tmpTime;

                // 3.8 调用 decoder 进行 MLP 解码，更新边 logit、label、accuracy
                tmpTime = System.currentTimeMillis();
                graphX.decoder(srcID, dstID);
                decoderTime += System.currentTimeMillis() - tmpTime;

                // 3.9 定期截断 RDD 血缘以防止内存溢出
                if (num % CHECKPOINT_FREQUENCY == 0) {
                    graphX.getGraph().cache();
                    graphX.getGraph().checkpoint();
                }

                // 3.10 评估结果，并释放部分内存
                tmpTime = System.currentTimeMillis();
                count += graphX.evaluate(srcID, dstID, num);
                SPARK_INIT.unpersistAll(num);
                evaluateTime += System.currentTimeMillis() - tmpTime;

                System.out.println("num = " + ++num);
                System.out.println("count = " + count);

                eRDD = graphX.getGraph().edges();
                vRDD = graphX.getGraph().vertices();
            }
            bufferedReader.close();

            // 4. 输出结果（耗时、准确率）
            long endTime = System.currentTimeMillis();
            float n = num;
            log.warn("--- {} 推理结束", new SimpleDateFormat(DATE_FORMAT).format(endTime));
            log.warn("--- total:    {} ms", endTime - startTime);
            log.warn("--- create:   {} ms, avg: {} ms", createGraphTime, createGraphTime / n);
            log.warn("--- merge:    {} ms, avg: {} ms", mergeTime, mergeTime / n);
            log.warn("--- updateTs: {} ms, avg: {} ms", updateTsTime, updateTsTime / n);
            log.warn("--- neighbor: {} ms, avg: {} ms", genNeighborTime, genNeighborTime / n);
            log.warn("--- encoder:  {} ms, avg: {} ms", encoderTime, encoderTime / n);
            log.warn("--- mailbox:  {} ms, avg: {} ms", updateMailboxTime, updateMailboxTime / n);
            log.warn("--- decoder:  {} ms, avg: {} ms", decoderTime, decoderTime / n);
            log.warn("--- evaluate: {} ms, avg: {} ms", evaluateTime, evaluateTime / n);
            log.warn("--- num:      {}", n);
            log.warn("--- count:    {}", count);
            log.warn("--- accuracy: {}", 1 - (count * 1.0) / (n * 2));

            // 5. 保存推理后全图点特征（可选）
            log.warn("--- {} 开始保存点特征", new SimpleDateFormat(DATE_FORMAT).format(System.currentTimeMillis()));
            graphX.saveVertexFeat();
            log.warn("--- {} 保存 csv 结束", new SimpleDateFormat(DATE_FORMAT).format(System.currentTimeMillis()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
