import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;


public class WC_Mapper extends Mapper<Object, Text, Text, Text> {
    private Text word = new Text();
    private Text fileNameAndOne = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Lấy tên file từ input split
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

        // Tách các từ trong dòng
        String[] tokens = value.toString().split("\\s+");
        for (String token : tokens) {
            word.set(token.toLowerCase()); // Chuyển tất cả từ về chữ thường
            fileNameAndOne.set(fileName + ":1");
            context.write(word, fileNameAndOne);
        }
    }
}
