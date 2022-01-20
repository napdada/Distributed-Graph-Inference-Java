package experiment;

import config.Constants;
import lombok.Getter;
import lombok.Setter;
import model.*;

/**
 * @author napdada
 * @version : v 0.1 2022/1/14 16:41
 */
@Getter
@Setter
public class Infer {

    private Encoder encoder;

    private Decoder decoder;

    private EncoderInput encoderInput;

    private DecoderInput decoderInput;

    private EncoderOutput encoderOutput;

    private DecoderOutput decoderOutput;
    int batchSize = 200, dim = 172;
    float[][] feat = new float[batchSize][dim];
    float[][][] mail = new float[batchSize][Constants.MAILBOX_LEN][dim];
    float[] lastUpdate = new float[batchSize], timestamp = new float[batchSize];
    float[][] posEmb = new float[batchSize][dim * 2], negEmb = new float[batchSize][dim * 2];
    float[] posLabel = new float[posEmb.length], negLabel = new float[negEmb.length];

    public Infer() {
        encoder = Encoder.getInstance();
        decoder = Decoder.getInstance();

    }

    public void doInfer() {

        encoderInput = new EncoderInput(feat, mail, lastUpdate, timestamp);
        encoderOutput = encoder.infer(encoderInput);


        decoderInput = new DecoderInput(posEmb, posLabel, negEmb, negLabel);
        decoderOutput = decoder.infer(decoderInput);
    }

    public static void main(String[] args) {
        int num = 10;
        Constants constants = new Constants(args);
        Infer infer = new Infer();
        for (int i = 0; i < 10; i++) {
            infer.doInfer();
        }
        long start = System.currentTimeMillis();
        for (int i = 0; i < num; i++) {
            System.out.println(i);
            infer.doInfer();
        }
        System.out.println((System.currentTimeMillis() - start) / (num * 1.0));
    }
}
