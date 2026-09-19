import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieRatingCount {

    public static class MovieMapper
            extends Mapper<LongWritable, org.apache.hadoop.io.Text,
                           LongWritable, IntWritable> {

        private final IntWritable ONE = new IntWritable(1);
        private final LongWritable movieId = new LongWritable();

        @Override
        public void map(LongWritable key,
                        org.apache.hadoop.io.Text value,
                        Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Skip CSV header
            if (line.startsWith("userId,movieId,rating,timestamp")) {
                return;
            }

            if (!line.trim().isEmpty()) {

                String[] fields = line.split(",");

                if (fields.length >= 2) {
                    try {
                        long id = Long.parseLong(fields[1]);

                        movieId.set(id);
                        context.write(movieId, ONE);

                    } catch (NumberFormatException e) {
                        // Ignore invalid records
                    }
                }
            }
        }
    }

    public static class MovieReducer
            extends Reducer<LongWritable, IntWritable,
                           LongWritable, IntWritable> {

        private final IntWritable result = new IntWritable();

        @Override
        public void reduce(LongWritable key,
                           Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable value : values) {
                sum += value.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println(
                "Usage: MovieRatingCount <input path> <output path>"
            );
            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Movie Rating Count");

        job.setJarByClass(MovieRatingCount.class);

        job.setMapperClass(MovieMapper.class);
        job.setReducerClass(MovieReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(
            job, new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
            job, new Path(args[1])
        );

        System.exit(
            job.waitForCompletion(true) ? 0 : 1
        );
    }
}