import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.training.evaluator.Accuracy;
import dataset.*;
import model.Encoder;

import java.io.*;
import java.util.Arrays;

import static config.Constants.*;

/**
 * @author napdada
 * @version : v 0.1 2021/11/12 4:22 下午
 */
public class Test {

    public void reddit() {
        try {
            File datasetCsv = new File(DATASET_PATH);
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
        Encoder encoder = Encoder.getInstance();

        System.out.println("-------- 开始读取数据并构图 --------");
        GraphX graphX = new GraphX();
        graphX.readData();
        graphX.printAll();

        System.out.println("--------  测试 mergeEdges --------");
        graphX.mergeEdges();
        graphX.printAll();

        System.out.println("--------  测试 updateTimestamp --------");
        graphX.updateTimestamp((long) 0, (long) 1, 1);
        graphX.printAll();

        System.out.println("-------- 测试 genNeighbor --------");
        graphX.event2DSubgraph((long) 0, (long) 1);
        graphX.printAll();

        Long vertexID = 0L;
        Long vertex2ID = 1L;
        Long vertex3ID = 14L;

        System.out.println("--------  测试 encoder --------");
        graphX.encoder(vertexID, vertex2ID);
        graphX.printAll();

        System.out.println("--------  测试 decoder --------");
        graphX.decoder(vertexID, vertex2ID);
        graphX.printAll();

        System.out.println("--------  测试 updateMailbox --------");
        graphX.updateMailbox();
        graphX.printVertexs();
    }
    public static void main(String[] args) {
        Test test = new Test();
//        test.reddit();

//        test.test();
        try(NDManager manager = NDManager.newBaseManager()) {
            float[] logit = {(float) 0.4, (float) 0.4, 0, 0, 0};
            float[] label = {0, 1, 0, 0, 0};
            NDArray logits = manager.create(logit);
            NDArray labels = manager.create(label);
            Accuracy accuracy = new Accuracy();
            long[] acc = accuracy.evaluate(new NDList(labels), new NDList(logits)).toLongArray();
            System.out.println(Arrays.toString(acc));
        }

    }
}
