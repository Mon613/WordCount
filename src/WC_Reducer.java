import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class WC_Reducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<String, Integer> fileCountMap = new HashMap<>();
        int totalCount = 0;

        // Duyệt qua các giá trị và phân tích thông tin file và số lượng
        for (Text val : values) {
            String[] fileAndCount = val.toString().split(":");
            String fileName = fileAndCount[0];
            int count = Integer.parseInt(fileAndCount[1]);

            fileCountMap.put(fileName, fileCountMap.getOrDefault(fileName, 0) + count);
            totalCount += count;
        }

        // Xây dựng chuỗi kết quả
        StringBuilder sb = new StringBuilder("[");
        for (String file : fileCountMap.keySet()) {
            sb.append(file).append(", ");
        }
        sb.setLength(sb.length() - 2); // Xóa dấu phẩy cuối cùng
        sb.append("], ").append(totalCount);

        result.set(sb.toString());
        context.write(key, result);
    }
}
