package model;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.nn.Activation;
import ai.djl.translate.Batchifier;
import ai.djl.translate.Translator;
import ai.djl.translate.TranslatorContext;

import static config.Constants.*;

/**
 * 处理 Decoder 模型输入输出
 * @author napdada
 * @version : v 0.1 2021/12/7 18:58
 */
public class DecoderTranslator implements Translator<DecoderInput, DecoderOutput> {

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
     * 处理 Pytorch Decoder 模型输入
     * @param ctx TranslatorContext
     * @param input 自定义的模型输入 DecoderInput
     * @return ndList
     */
    @Override
    public NDList processInput(TranslatorContext ctx, DecoderInput input) {
        NDManager manager = ctx.getNDManager();
        NDArray embedding1 = manager.create(input.getPosEmb()), embedding2 = manager.create(input.getNegEmb());
        NDArray label1 = manager.create(input.getPosLabel()), label2 = manager.create(input.getNegLabel());
        boolean isPos = (input.getPosLabel()[0] == 0f);
        if (!TASK_NAME.equals("LP")) {
            embedding2 = manager.create(0);
        }
        embedding1.setName(isPos ? POS_EMB : NEG_EMB);
        embedding2.setName(isPos ? NEG_EMB : POS_EMB);
        label1.setName(isPos ? POS_LABEL : NEG_LABEL);
        label2.setName(isPos ? NEG_LABEL : POS_LABEL);
        return new NDList(embedding1, label1, embedding2, label2);

    }

    /**
     * 处理 Pytorch Decoder 模型输出
     * @note 在 Translator 中创建的 NDList 将在 predict() 之后立即销毁，因此需要自定义模型输出，并且调用 duplicate() 防止浅拷贝
     * @param ctx TranslatorContext
     * @param list NDList
     * @return 自定义的模型输出 DecoderOutput
     */
    @Override
    public DecoderOutput processOutput(TranslatorContext ctx, NDList list) {
        float[] logic = Activation.sigmoid(list.get(0).duplicate()).toFloatArray();
        int[] tmp;
        float[] label;
        if (list.get(1).getDataType().name().equals("INT32")) {
            tmp =  list.get(1).duplicate().toIntArray();
            label = new float[tmp.length];
            for (int i = 0; i < tmp.length; i++) {
                label[i] = tmp[i];
            }
        } else {
            label = list.get(1).duplicate().toFloatArray();
        }
        return new DecoderOutput(logic, label);
    }
}
