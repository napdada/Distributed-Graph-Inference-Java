import config.Constants;
import config.SparkInit;
import dataset.Dataset;
import dataset.Edata;
import dataset.Vdata;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.io.*;
import java.text.SimpleDateFormat;

/**
 * @author napdada
 * @version : v 0.1 2021/11/30 8:48 下午
 */
@Slf4j
public class Main {
    public static void main(String[] args) {
        try {
            // 1. Spark 初始化
            long sparkInitTime = System.currentTimeMillis();
            SparkInit sparkInit = new SparkInit();
            JavaSparkContext sc = sparkInit.getSparkContext();
            log.error("----------------- Spark 初始化耗时：{} ms ----------------", System.currentTimeMillis() - sparkInitTime);

            // 2. 初始化数据集配置、图配置、模型配置
            long initTime = System.currentTimeMillis(), createGraphTime = 0, mergeTime = 0, updateTsTime = 0, genNeighborTime = 0,
                    inferTime = 0, updateMailboxTime = 0, decoderTime = 0, actionTime = 0, tmpTime;
            int num = 1;
            File datasetCsv = new File(Constants.DATASET_PATH);
            BufferedReader bufferedReader = new BufferedReader(new FileReader(datasetCsv));
            Dataset dataset = new Dataset(sc);
            RDD<Tuple2<Object, Vdata>> vertexRDD = null;
            RDD<Edge<Edata>> edgeRDD = null;
            String lineData;
            String[] line;
            long srcID, dstID;
            float timestamp;
            int count = 0;
            log.error("----------------- 初始化配置耗时：{} ms ----------------", System.currentTimeMillis() - initTime);

            // 3. 图推理
            bufferedReader.readLine();
            long startTime = System.currentTimeMillis();
            log.error("----------------- {} 开始推理 ----------------", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(startTime));
            while ((lineData = bufferedReader.readLine()) != null) {
                // 读取一个新的事件，并与历史事件一起构图
                tmpTime = System.currentTimeMillis();
                line = lineData.split(",");
                srcID = Long.parseLong(line[Constants.SRC_ID_INDEX]);
                dstID = Long.parseLong(line[Constants.DST_ID_INDEX]);
                timestamp = Float.parseFloat(line[Constants.TIMESTAMP_INDEX]);
                if (edgeRDD == null) {
                    edgeRDD = sc.parallelize(dataset.eventToEdge(line)).rdd();
                } else {
                    edgeRDD = edgeRDD.union(sc.parallelize(dataset.eventToEdge(line)).rdd());
                }
                // creatGraph
                dataset.creatGraph(vertexRDD, edgeRDD);
                createGraphTime += System.currentTimeMillis() - tmpTime;

                // mergeEdges
                tmpTime = System.currentTimeMillis();
                dataset.mergeEdges();
                mergeTime += System.currentTimeMillis() - tmpTime;

                // updateTimestamp
                tmpTime = System.currentTimeMillis();
                dataset.updateTimestamp(srcID, dstID, timestamp);
                updateTsTime += System.currentTimeMillis() - tmpTime;

                // genNeighbor
                tmpTime = System.currentTimeMillis();
                dataset.genNeighbor();
                genNeighborTime += System.currentTimeMillis() - tmpTime;

                // infer
                tmpTime = System.currentTimeMillis();
                dataset.encoder(srcID, dstID);
                inferTime += System.currentTimeMillis() - tmpTime;

                // updateMailbox
                tmpTime = System.currentTimeMillis();
                dataset.updateMailbox();
                updateMailboxTime += System.currentTimeMillis() - tmpTime;

                // decoder
                tmpTime = System.currentTimeMillis();
                dataset.decoder(timestamp);
                decoderTime += System.currentTimeMillis() - tmpTime;

                tmpTime = System.currentTimeMillis();
                edgeRDD = dataset.getGraph().edges();
                vertexRDD = dataset.getGraph().vertices();
                actionTime += System.currentTimeMillis() - tmpTime;

//                if (num % 10 == 0) {
//                    dataset.getGraph().cache();
//                    dataset.getGraph().checkpoint();
//                    count += dataset.evaluate();
//                }
                System.out.println(num++);
            }
            bufferedReader.close();
            long endTime = System.currentTimeMillis();
            log.error("----------------- {} 推理结束 ----------------", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(endTime));
            log.error("----------------- 推理耗时: {} ms ----------------", endTime-startTime);
            log.error("----------------- createGraphTime: {} ms, avg: {} ms ----------------", createGraphTime, createGraphTime / num);
            log.error("----------------- mergeTime: {} ms, avg: {} ms ----------------", mergeTime, mergeTime / num);
            log.error("----------------- updateTsTime: {} ms, avg: {} ms ----------------", updateTsTime, updateTsTime / num);
            log.error("----------------- genNeighborTime: {} ms, avg: {} ms ----------------", genNeighborTime, genNeighborTime / num);
            log.error("----------------- inferTime: {} ms, avg: {} ms ----------------", inferTime, inferTime / num);
            log.error("----------------- updateMailboxTime: {} ms, avg: {} ms ----------------", updateMailboxTime, updateMailboxTime / num);
            log.error("----------------- decoderTime: {} ms, avg: {} ms ----------------", decoderTime, decoderTime / num);
            log.error("----------------- actionTime: {} ms, avg: {} ms ----------------", actionTime, actionTime / num);

            // 统计 acc
            tmpTime = System.currentTimeMillis();
            dataset.printAll();
//            double accuracy = 1 - dataset.evaluate() / num;
//            log.error("----------------- accuracy: {}  ----------------", accuracy);
            log.error("----------------- count: {}  ----------------", count);
            log.error("----------------- 统计 acc: {} ms ----------------", System.currentTimeMillis() - tmpTime);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
