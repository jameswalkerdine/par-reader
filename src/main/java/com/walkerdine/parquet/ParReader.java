package com.walkerdine.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.tools.read.SimpleReadSupport;
import org.apache.parquet.tools.read.SimpleRecord;
//import org.apache.parquet.hadoop.DictionaryPageReader;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class ParReader implements  Runnable{

    public static void main(String[] args) {
        ExecutorService es = Executors.newSingleThreadExecutor();
        es.submit( new ParReader());
    }

    @Override
    public void run() {

        ParquetReader<SimpleRecord> reader = null;
        try {
            Configuration conf = new Configuration();
            Path inputPath = new Path("/home/james/uber/uber2.parquet/part-r-00000-9850f367-b3e8-41ba-8d44-1cc7f471a032.snappy.parquet");
            reader = ParquetReader.builder(new SimpleReadSupport(), inputPath).build();
            FileStatus inputFileStatus = new Path("/home/james/uber/uber2.parquet/part-r-00000-9850f367-b3e8-41ba-8d44-1cc7f471a032.snappy.parquet").getFileSystem(conf).getFileStatus(inputPath);
ParquetFileReader fr = ParquetFileReader.open(new Configuration(), inputPath);
            List<Footer> footers = ParquetFileReader.readFooters(new Configuration(), inputFileStatus, false);
            for (Footer f : footers) {
                List<BlockMetaData> blocks = f.getParquetMetadata().getBlocks();
                for (BlockMetaData block : blocks) {
                   System.out.println(block.toString());
                    //DictionaryPageReader dpr = new DictionaryPageReader(reader, block)
    //                DictionaryPageReadStore dpr = fr.getDictionaryReader( block);
      //              System.out.println(dpr);

                    System.out.println("start: "+ block.getStartingPos() + "  end: "
                            + (block.getStartingPos()
                            + block.getCompressedSize())
                            + "  rows: " +block.getRowCount());


                }
            }


            for (SimpleRecord value = reader.read(); value != null; value = reader.read()) {
                System.out.println(value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (Exception ex) {
                }
            }
        }

    }
}
