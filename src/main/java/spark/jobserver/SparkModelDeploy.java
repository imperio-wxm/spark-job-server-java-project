package spark.jobserver;

import com.typesafe.config.Config;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.util.ArrayList;
import java.util.List;

import spark.jobserver.api.JobEnvironment;
import spark.jobserver.japi.JSparkJob;


/**
 * SparkModelDeploy
 *
 * @author mma
 * @since Jan, 2019
 */
public class SparkModelDeploy implements JSparkJob<String> {
    public String run(final JavaSparkContext sc, final JobEnvironment runtime, final Config data) {
        SQLContext sqlContext = new SQLContext(sc);
        String comments = data.getString("input.comments");

        List<Row> rows = new ArrayList<Row>();
        rows.add(RowFactory.create(comments));
        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("text", DataTypes.StringType, true));
        Dataset<Row> rowsDataset = sqlContext.createDataFrame(rows, DataTypes.createStructType(fields));
        rowsDataset.show();

        // Tokenizer
        RegexTokenizer tokenizer = new RegexTokenizer()
                .setInputCol("text")
                .setOutputCol("words")
                .setPattern("\\p{L}+")
                .setGaps(false);
        Dataset<Row> dataInput = tokenizer.transform(rowsDataset);
        dataInput.show();

        // Stop words remover
        StopWordsRemover remover = new StopWordsRemover()
                .setInputCol("words")
                .setOutputCol("filtered");
        dataInput = remover.transform(dataInput);
        dataInput.show(false);

        // Conunt Vectorizer
        String cvmodelPath = data.getString("input.cvmodel");
        //hdfs://localhost:10000/tmp/data/20190210/cvmodel
        CountVectorizerModel cv = CountVectorizerModel
                .load(cvmodelPath)
                .setInputCol("filtered")
                .setOutputCol("features");
        String[] topWords = cv.vocabulary();
        System.out.print("topwords/n" + topWords.toString());
        dataInput = cv.transform(dataInput);
        dataInput.show(false);

        // Load Model
        String lrmodelPath = data.getString("input.lrmodel");
        //"hdfs://localhost:10000//tmp/data/20190210/lrmodel"
        LogisticRegressionModel model = LogisticRegressionModel.read().load(lrmodelPath);
        //Pipeline pipeline = new Pipeline().setStages(
        //        new PipelineStage[] {tokenizer, remover, cv, model});
        //pipeline.fit(data);

        Dataset<Row> result = model.transform(dataInput.select("text","features"));

        result.show(false);

        for (Row row : result.select("prediction").collectAsList()) {
            double prediction = row.getDouble(0);
            if (prediction >= 1) {
                return "Positive";
            } else {
                return "Negative";
            }
        }
        return "Failed";
    }

    public Config verify(final JavaSparkContext sc, final JobEnvironment runtime, final Config config) {
        return config;
    }
}
