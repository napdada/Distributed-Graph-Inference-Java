import config.Constants;
import config.SparkInit;
import dataset.*;
import model.Encoder;

import java.io.*;

/**
 * @author napdada
 * @version : v 0.1 2021/11/12 4:22 下午
 */
public class Test {

    public void reddit() {
        try {
            File datasetCsv = new File(Constants.DATASET_PATH);
            BufferedReader bufferedReader = new BufferedReader(new FileReader(datasetCsv));
            File result = new File("./src/main/resources/dataset/reddit_java.csv");
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(result));
            bufferedWriter.write("index, src, dst, timestamp, label, feat");
            bufferedWriter.flush();

            String lineData;
            String[] line;
            Long n1, n2, n3;
            float n4;
            int n5;
            bufferedReader.readLine();
            while ((lineData = bufferedReader.readLine()) != null) {
                line = lineData.replace("\"","").replace("[","").replace("]", "").split(",");
                n1 = Long.parseLong(line[0]);
                n2 = Long.parseLong(line[1]);
                n3 = Long.parseLong(line[2]);
                n4 = Float.parseFloat(line[3]);
                n5 = (int)Float.parseFloat(line[4]);
                StringBuilder sb = new StringBuilder();
                for (int i = 5; i < 5 + 172 - 1; i++) {
                    sb.append(line[i]).append(",");
                }
                sb.append(line[5 + 172 - 1]);

                bufferedWriter.newLine();
                bufferedWriter.write(n1 + ",");
                bufferedWriter.write(n2 + ",");
                bufferedWriter.write(n3 + ",");
                bufferedWriter.write(n4 + ",");
                bufferedWriter.write(n5 + ",");
                bufferedWriter.write(sb.toString());
                bufferedWriter.flush();
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }

    public void test() {
        System.out.println("-------- Spark init --------");
        SparkInit sparkInit = new SparkInit();
        Encoder encoder = Encoder.getInstance();

        System.out.println("-------- 开始读取数据并构图 --------");
        Dataset dataset = new Dataset(Constants.DATASET_PATH, sparkInit.getSparkContext());
        dataset.readData();
        dataset.printAll();

        System.out.println("--------  测试 mergeEdges --------");
        dataset.mergeEdges();
        dataset.printAll();

        System.out.println("--------  测试 updateTimestamp --------");
        dataset.updateTimestamp((long) 0, (long) 1, 1);
        dataset.printAll();

        System.out.println("-------- 测试 genNeighbor --------");
        dataset.genNeighbor();
        dataset.printAll();

        Long vertexID = 0L;
        Long vertex2ID = 1L;
        Long vertex3ID = 14L;

        System.out.println("--------  测试 encoder --------");
        dataset.encoder(vertexID, vertex2ID);
        dataset.printAll();

        System.out.println("--------  测试 decoder --------");
        dataset.decoder(1f);
        dataset.printAll();

        System.out.println("--------  测试 updateMailbox --------");
        dataset.updateMailbox();
        dataset.printVertexs();
    }
    public static void main(String[] args) {
        Test test = new Test();
//        test.reddit();

        test.test();
    }
}
