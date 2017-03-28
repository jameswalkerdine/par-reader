package com.walkerdine.parquet;

import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.lz4.Lz4Decompressor;
import org.apache.parquet.VersionParser;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.*;
import org.apache.parquet.hadoop.*;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.tools.read.SimpleReadSupport;
import org.apache.parquet.tools.read.SimpleRecord;
//import org.apache.parquet.hadoop.DictionaryPageReader;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;



public class ParReader implements  Runnable {


    class Vis implements DataPage.Visitor {



        @Override
        public Object visit(DataPageV1 dataPageV1) {
            Decompressor lz = new DefaultCodec().createDecompressor();


            try {
                BytesInput bts = dataPageV1.getBytes();
                InputStream is = bts.toInputStream();
                int n = is.read();
                int nn = lz.decompress(dataPageV1.getBytes().toByteArray(), 0, dataPageV1.getUncompressedSize());
                return null;
                //return new DataPageV1(lz.decompress(dataPageV1.getBytes().toByteArray(), 0, dataPageV1.getUncompressedSize()), dataPageV1.getValueCount(), dataPageV1.getUncompressedSize(), dataPageV1.getStatistics(), dataPageV1.getRlEncoding(), dataPageV1.getDlEncoding(), dataPageV1.getValueEncoding());
            } catch (IOException var3) {
                throw new ParquetDecodingException("could not decompress page", var3);
            }
        }

        @Override
        public Object visit(DataPageV2 dataPageV2) {
            return null;
        }
    }

    public static void main(String[] args) {
        ExecutorService es = Executors.newSingleThreadExecutor();
        es.submit(new ParReader());
    }

    @Override
    public void run() {

        ParquetReader<SimpleRecord> reader = null;

        try {
            String loc2 = "c:/tmp/expected.parquet";
            String loc = "/home/james/testdata/expected.parquet";
            Configuration conf = new Configuration();
            Path inputPath = new Path(loc);
            reader = ParquetReader.builder(new SimpleReadSupport(), inputPath).build();
            // FileStatus inputFileStatus = new Path().getFileSystem(conf).getFileStatus(inputPath);
            ParquetFileReader fr = ParquetFileReader.open(new Configuration(), inputPath);
            FileStatus inputFileStatus = new Path(loc).getFileSystem(conf).getFileStatus(inputPath);

            PageReadStore rg = fr.readNextRowGroup();
            List<BlockMetaData> blocks2 = fr.getFooter().getBlocks();
            BlockMetaData b = blocks2.get(0);

            List<Footer> footers = ParquetFileReader.readFooters(new Configuration(), inputFileStatus, false);


            List<ColumnChunkMetaData> cols = b.getColumns();
            ColumnPath pathKey = cols.get(0).getPath();
            Map<ColumnPath, ColumnDescriptor> paths = (Map<ColumnPath, ColumnDescriptor>) FieldUtils.readField(fr, "paths", true);
            ColumnDescriptor columnDescriptor = paths.get(pathKey);

            PageReader x = rg.getPageReader(columnDescriptor);

            //DataPage pg = x.readPage();

            final List<String> actualValues = new ArrayList<String>();
            PrimitiveConverter converter = new PrimitiveConverter() {
                @Override
                public void addBinary(Binary value) {
                    actualValues.add(value.toStringUsingUTF8());
                }
            };

            ColumnReaderImpl columnReader = new ColumnReaderImpl(
                    columnDescriptor, x, converter,
                    new VersionParser.ParsedVersion("parquet-mr", "1.6.0", "abcd"));

            while (actualValues.size() < columnReader.getTotalValueCount()) {
                columnReader.writeCurrentValueToConverter();
                columnReader.consume();
            }


            //rs = new org.apache.parquet.hadoop.ColumnChunkPageReadStore(pg.getValueCount());

            //pg.accept(new Vis());
            // f.getParquetMetadata().getFileMetaData().getSchema()
            Set<String> flds = getFields(footers.get(0).getParquetMetadata().getFileMetaData().getSchema());
            for (Footer f : footers) {
                List<BlockMetaData> blocks = f.getParquetMetadata().getBlocks();
                for (BlockMetaData block : blocks) {
                    System.out.println(block.toString());
                    //DictionaryPageReader dpr = new DictionaryPageReader(reader, block)
                    //                DictionaryPageReadStore dpr = fr.getDictionaryReader( block);
                    //              System.out.println(dpr);

                    System.out.println("start: " + block.getStartingPos() + "  end: "
                            + (block.getStartingPos()
                            + block.getCompressedSize())
                            + "  rows: " + block.getRowCount());


                }
            }


            // need to just read do calc ea row group not the whole file
            for (SimpleRecord value = reader.read(); value != null; value = reader.read()) {
                addRecord(value, "event");
                //System.out.println(value);
            }
            System.out.println("values");
            for( Map.Entry<String, Set<String>> entry : nameValueSets.entrySet()) {
                System.out.println("values for ["+ entry.getKey() +"]");
                for(String value: entry.getValue()) {
                    System.out.print(value + ",");
                }
                System.out.println();

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


    void addRecord(SimpleRecord.NameValue value, String root) {
        if (value.getValue() instanceof String) {
            addValueFor(root + "-" + value.getName(), (String)value.getValue());
        } else {
            if (value.getValue() instanceof SimpleRecord) {
                SimpleRecord sm = (SimpleRecord) value.getValue();
                for (SimpleRecord.NameValue v : sm.getValues()) {

                    String s = "";
                    if( root.length() >0) {
                        s = root;
                    }
                    if( !(v.getName().equalsIgnoreCase("bag") || v.getName().equalsIgnoreCase("array") || v.getValue() instanceof  String)) {
                        s = s + "-" + v.getName();
                    }

                    addRecord(v, s);
                }
            }
        }
    }

    void addRecord(SimpleRecord value, String root) {
        for( SimpleRecord.NameValue nv : value.getValues())  {
            addRecord(nv, root);
        }
    }

    private void addValueFor(String s, String value) {
        System.out.println(s + "  " + value);



        Set<String> valueSet = nameValueSets.getOrDefault(s, new HashSet<String>(255));
        valueSet.add(value);
        nameValueSets.put(s, valueSet);

    }



    private Map<String, Set<String>> nameValueSets = new ConcurrentHashMap<>(1000);

    private Set<String> getFields(Object value, String root) {
        Set<String> result = new HashSet();
        if (value instanceof String) {
            Set<String> one = new HashSet();
            one.add((String) value);
            return one;
        } else {
            if (value instanceof SimpleRecord) {
                for (Object v : ((SimpleRecord) value).getValues()) {
                    if (v instanceof SimpleRecord.NameValue) {
                        SimpleRecord.NameValue nv = (SimpleRecord.NameValue) v;
                        result.addAll(getFields(nv.getValue(), root + "-" + nv.getName()));
                    }
                }
            }
        }
        return result;
    }

    private Set<String> getFields(MessageType schema) {
        Set<String> result = new HashSet<>();
        for (Type t : schema.getFields()) {
            result.addAll(getf(t, ""));
        }
        return result;
    }

    private Set<String> getf(Type t, String root) {
        Set<String> result = new HashSet<>();
        if( t.isPrimitive()) {
            if( root.isEmpty()) {
                result.add(t.getName());
            } else
            result.add(root + "-" + t.getName());
        } else {
            GroupType gt = t.asGroupType();
            for( Type nt : gt.getFields()) {
                String rooty;
                if( root.isEmpty()) rooty = ""; else rooty = root + "-";
                if( !gt.getName().equalsIgnoreCase("Bag") && !gt.getName().equalsIgnoreCase("Array"))  result.addAll(getf(nt, rooty+gt.getName()  ));
                else {   result.addAll(getf(nt, root  ));
                }
            }
        }
        return result;

    }
}



