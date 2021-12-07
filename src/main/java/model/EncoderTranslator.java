package model;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.Shape;
import ai.djl.translate.Batchifier;
import ai.djl.translate.Translator;
import ai.djl.translate.TranslatorContext;
import config.Constants;

import java.util.Arrays;

/**
 * @author napdada
 * @version : v 0.1 2021/11/29 8:46 下午
 */
public class EncoderTranslator implements Translator<EncoderInput, EncoderOutput> {

    /**
     * The Batchifier describes how to combine a batch together
     * Stacking, the most common batchifier, takes N [X1, X2, ...] arrays to a single [N, X1, X2, ...] array
     * @return Batchifier.STACK
     */
    @Override
    public Batchifier getBatchifier() {
        return null;
    }

    /**
     * 处理 Pytorch 模型输入
     * @param ctx TranslatorContext
     * @param input 自定义的模型输入 EncoderInput
     * @return ndList
     */
    @Override
    public NDList processInput(TranslatorContext ctx, EncoderInput input) {
        NDManager manager = ctx.getNDManager();
        NDArray featArray = manager.create(input.getFeat());
        NDArray mailArray = manager.create(input.flatMail(), new Shape(input.getMail().length, Constants.MAILBOX_LEN, input.getMailDim()));
        NDArray lastUpdateArray = manager.create(input.getLastUpdate());
        NDArray timestampArray = manager.create(input.getTimestamp());
        featArray.setName("n_feat");
        mailArray.setName("n_mail");
        lastUpdateArray.setName("n_last_update");
        timestampArray.setName("n_ts");
        return new NDList(featArray, mailArray, lastUpdateArray, timestampArray);
    }

    /**
     * 处理 Pytorch 模型输出
     * @note 在 Translator 中创建的 NDList 将在 predict() 之后立即销毁，因此需要自定义模型输出，并且调用 duplicate() 防止浅拷贝
     * @param ctx TranslatorContext
     * @param list NDList
     * @return 自定义的模型输出 EncoderOutput
     */
    @Override
    public EncoderOutput processOutput(TranslatorContext ctx, NDList list) {
        NDArray ndArray = list.get(0).duplicate();
        float[] output= ndArray.toFloatArray();
        int num = output.length / Constants.FEATURE_DIM, from, to;
        float[][] embedding = new float[num][];
        for (int i = 0; i < num; i++) {
            from = i * Constants.FEATURE_DIM;
            to = from + Constants.FEATURE_DIM;
            embedding[i] = Arrays.copyOfRange(output, from, to);
        }
        return new EncoderOutput(embedding);
    }

}
