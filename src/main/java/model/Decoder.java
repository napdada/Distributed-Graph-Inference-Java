package model;

import ai.djl.Model;
import ai.djl.inference.Predictor;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

import static config.Constants.*;

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

    private static Decoder decoder;

    private Decoder() {
        modelPath = MODEL_PATH;
        modelName = DECODER_NAME;
        model = Model.newInstance(modelName);
        Path modelDir = Paths.get(modelPath);
        try {
            model.load(modelDir);
            translator = new DecoderTranslator();
            predictor = model.newPredictor(translator);
        } catch (Exception e) {
            logger.error("Decoder(): " + e.getMessage());
        }
    }

    public static Decoder getInstance() {
        if (decoder == null) {
            synchronized (Decoder.class) {
                if (decoder == null) {
                    decoder = new Decoder();
                }
            }
        }
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
            logger.error("Decoder infer():" + e.getMessage());
            return null;
        }
    }
}
