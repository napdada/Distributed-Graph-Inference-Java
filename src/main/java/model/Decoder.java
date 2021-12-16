package model;

import ai.djl.Model;
import ai.djl.inference.Predictor;
import config.Constants;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Pytorch Decoder 模型定义
 *
 * @author napdada
 * @version : v 0.1 2021/12/7 18:58
 */
@Getter
@Setter
public class Decoder {
    /**
     * Log
     */
    private static final Logger logger = LoggerFactory.getLogger(Decoder.class);
    /**
     * Pytorch 模型保存路径
     */
    private String modelPath;
    /**
     * Pytorch 模型名称
     */
    private String modelName;
    /**
     * Pytorch encoder 模型
     */
    private Model model;
    /**
     * 模型输入 Translator
     */
    DecoderTranslator translator;
    /**
     * 模型预测
     */
    Predictor<DecoderInput, DecoderOutput> predictor;

    private static Decoder decoder = new Decoder();

    private Decoder() {
        modelPath = Constants.MODEL_PATH;
        modelName = Constants.DECODER_NAME;
        model = Model.newInstance(modelName);
        Path modelDir = Paths.get(modelPath);
        try {
            model.load(modelDir);
            translator = new DecoderTranslator();
            predictor = model.newPredictor(translator);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public static Decoder getInstance() {
        return decoder;
    }

    /**
     * 模型推理
     * @param decoderInput 模型输入
     * @return DecoderOutput 模型输出
     */
    public DecoderOutput infer(DecoderInput decoderInput) {
        try {
            return predictor.predict(decoderInput);
        } catch (Exception e) {
            logger.error(e.getMessage());
            return null;
        }
    }
}
