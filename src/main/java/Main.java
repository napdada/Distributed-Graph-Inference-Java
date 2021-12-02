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
            SparkInit sparkInit = new SparkInit(Constants.SPARK_APP_NAME, Constants.SPARK_MASTER);
            JavaSparkContext sc = sparkInit.getSparkContext();
            log.error("----------------- Spark 初始化耗时：{} ms ----------------", System.currentTimeMillis() - sparkInitTime);

            // 2. 初始化数据集配置、图配置、模型配置
            long initTime = System.currentTimeMillis(), createGraphTime = 0, mergeTime = 0, updateTsTime = 0, genNeighborTime = 0,
                    inferTime = 0, updateMailboxTime = 0, tmpTime;
            int i = 1;
            File datasetCsv = new File(Constants.DATASET_PATH);
            BufferedReader bufferedReader = new BufferedReader(new FileReader(datasetCsv));
            Dataset dataset = new Dataset(sc);
            RDD<Tuple2<Object, Vdata>> vertexRDD = null;
            RDD<Edge<Edata>> edgeRDD = null;
            String lineData;
            String[] line;
            long srcID, dstID;
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
                if (edgeRDD == null) {
                    edgeRDD = sc.parallelize(dataset.eventToEdge(line)).rdd();
                } else {
                    edgeRDD = edgeRDD.union(sc.parallelize(dataset.eventToEdge(line)).rdd());
                }
                dataset.creatGraph(vertexRDD, edgeRDD);
                createGraphTime += System.currentTimeMillis() - tmpTime;

                // mergeEdges
                tmpTime = System.currentTimeMillis();
                dataset.mergeEdges();
                mergeTime += System.currentTimeMillis() - tmpTime;

                // updateTimestamp
                tmpTime = System.currentTimeMillis();
                dataset.updateTimestamp();
                updateTsTime += System.currentTimeMillis() - tmpTime;

                // genNeighbor
                tmpTime = System.currentTimeMillis();
                dataset.genNeighbor();
                genNeighborTime += System.currentTimeMillis() - tmpTime;

                // infer
                tmpTime = System.currentTimeMillis();
                dataset.infer(srcID, dstID);
                inferTime += System.currentTimeMillis() - tmpTime;

                // updateMailbox
                tmpTime = System.currentTimeMillis();
                dataset.updateMailbox();
                updateMailboxTime += System.currentTimeMillis() - tmpTime;

                vertexRDD = dataset.getGraph().vertices();
                System.out.println(i++);
                if (i == 1000) break;
            }
            bufferedReader.close();
            long endTime = System.currentTimeMillis();
            log.error("----------------- {} 推理结束 ----------------", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(endTime));
            System.out.println("推理耗时： "+ (endTime-startTime) +"ms");
            float num = i;
            log.error("----------------- createGraphTime: {} ms, avg: {} ms ----------------", createGraphTime, createGraphTime / num);
            log.error("----------------- mergeTime: {} ms, avg: {} ms ----------------", mergeTime, mergeTime / num);
            log.error("----------------- updateTsTime: {} ms, avg: {} ms ----------------", updateTsTime, updateTsTime / num);
            log.error("----------------- genNeighborTime: {} ms, avg: {} ms ----------------", genNeighborTime, genNeighborTime / num);
            log.error("----------------- inferTime: {} ms, avg: {} ms ----------------", inferTime, inferTime / num);
            log.error("----------------- updateMailboxTime: {} ms, avg: {} ms ----------------", updateMailboxTime, updateMailboxTime / num);

            // 存储点特征结果，写入 csv
            tmpTime = System.currentTimeMillis();
            File result = new File(Constants.RESULT_PATH);
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(result));
            dataset.saveVertexFeat(bufferedWriter);
            log.error("----------------- writeTIme: {} ms ----------------", System.currentTimeMillis() - tmpTime);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
