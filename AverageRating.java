import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageRating {

    public static class RatingMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private static final Text KEY = new Text("rating");
        private DoubleWritable ratingValue = new DoubleWritable();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Skip CSV header
            if (line.startsWith("userId,movieId,rating,timestamp")) {
                return;
            }

            if (!line.trim().isEmpty()) {
                String[] fields = line.split(",");

                if (fields.length >= 3) {
                    try {
                        double rating = Double.parseDouble(fields[2]);
                        ratingValue.set(rating);
                        context.write(KEY, ratingValue);
                    } catch (NumberFormatException e) {
                        // Ignore invalid records
                    }
                }
            }
        }
    }

    public static class AverageReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values,
                            Context context)
                throws IOException, InterruptedException {

            double sum = 0.0;
            long count = 0;

            for (DoubleWritable value : values) {
                sum += value.get();
                count++;
            }

            double average = sum / count;

            result.set(average);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println(
                "Usage: AverageRating <input path> <output path>"
            );
            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Average Rating");

        job.setJarByClass(AverageRating.class);

        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(AverageReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}