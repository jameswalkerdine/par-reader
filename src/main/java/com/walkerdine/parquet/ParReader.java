package com.walkerdine.parquet;

import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DecompressorStream;
import org.apache.parquet.VersionParser;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.*;
import org.apache.parquet.column.values.dictionary.DictionaryValuesReader;
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesReader;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.tools.command.DumpCommand;
import org.apache.parquet.tools.read.SimpleReadSupport;
import org.apache.parquet.tools.read.SimpleRecord;
import org.apache.parquet.column.values.plain.PlainValuesReader;
//import org.apache.parquet.hadoop.DictionaryPageReader;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.System.out;


public class ParReader implements Runnable {

    public static void main(String[] args) {
        ExecutorService es = Executors.newSingleThreadExecutor();
        es.submit(new ParReader());
    }

    //TODO need to deal with morw than just strings endpoint2.port is an int for a start
    @Override
    public void run() {

        ParquetReader<SimpleRecord> reader = null;
        try {
            String loc2 = "c:/tmp/expected.parquet";
            String loc = "/home/james/testdata/expected.parquet";
            Configuration conf = new Configuration();
            Path inputPath = new Path(loc2);
            reader = ParquetReader.builder(new SimpleReadSupport(), inputPath).build();
            // FileStatus inputFileStatus = new Path().getFileSystem(conf).getFileStatus(inputPath);
            ParquetFileReader fr = ParquetFileReader.open(new Configuration(), inputPath);
            FileStatus inputFileStatus = new Path(loc2).getFileSystem(conf).getFileStatus(inputPath);


            List<Footer> footers = ParquetFileReader.readFooters(new Configuration(), inputFileStatus, false);
            // f.getParquetMetadata().getFileMetaData().getSchema()
            Set<String> flds = getFields(footers.get(0).getParquetMetadata().getFileMetaData().getSchema());
            for (Footer f : footers) {
                List<BlockMetaData> blocks = f.getParquetMetadata().getBlocks();
                for (BlockMetaData block : blocks) {
                    out.println(block.toString());
                    Object dpr = fr.getDictionaryReader(block);

                    List<ColumnChunkMetaData> cols = block.getColumns();
                    ColumnPath pathKey = cols.get(13).getPath();
                    Map<ColumnPath, ColumnDescriptor> paths = (Map<ColumnPath, ColumnDescriptor>) FieldUtils.readField(fr, "paths", true);
                    ColumnDescriptor columnDescriptor = paths.get(pathKey);

                    PageReadStore rowGroup = fr.readNextRowGroup();
                    PageReader simpleCol = rowGroup.getPageReader(columnDescriptor);

                    DataPage.Visitor visi = new DataPage.Visitor() {
                        @Override
                        public DataPage visit(DataPageV1 dataPageV1) {
                            CodecFactory codecFactory = new CodecFactory(new Configuration(), 0);

                            CodecFactory.BytesDecompressor decompressor = codecFactory.getDecompressor(CompressionCodecName.GZIP);

                            RunLengthBitPackingHybridValuesReader rle = new RunLengthBitPackingHybridValuesReader(8);
                            //PlainValuesReader.IntegerPlainValuesReader vr = new PlainValuesReader.IntegerPlainValuesReader.IntegerPlainValuesReader();
                            PlainValuesReader.LongPlainValuesReader vr = new PlainValuesReader.LongPlainValuesReader.LongPlainValuesReader();

                            try {
                                rle.initFromPage(dataPageV1.getValueCount(), dataPageV1.getBytes().toByteArray(), 0);
                                System.out.println(rle.readInteger());
                            } catch (IOException e) {
                                e.printStackTrace();
                            }

//                            try {
//                                BytesInput nn = decompressor.decompress(dataPageV1.getBytes(), dataPageV1.getUncompressedSize());
//                                System.out.println();
//                            } catch (IOException e) {
//                                e.printStackTrace();
//                            }
                            try {
                                DataPageV1 xxx = new DataPageV1(
                                        decompressor.decompress(dataPageV1.getBytes(), dataPageV1.getUncompressedSize()),
                                        dataPageV1.getValueCount(),
                                        dataPageV1.getUncompressedSize(),
                                        dataPageV1.getStatistics(),
                                        dataPageV1.getRlEncoding(),
                                        dataPageV1.getDlEncoding(),
                                        dataPageV1.getValueEncoding());

                                return xxx;
                            } catch (IOException e) {
                                return null;

                            }


//                            BytesInput bytes2 = dataPageV1.getBytes();
//                            try {
//                                CodecFactory codecFactory = new CodecFactory(new Configuration(), 0);
//                                DecompressorStream ds = (DecompressorStream)FieldUtils.readField(bytes2, "in", true);
//                                System.out.println(ds);
//                                Decompressor dec = (Decompressor)FieldUtils.readField(ds, "decompressor", true);
//                                for(int x=1; x< dataPageV1.getValueCount(); x++) {
//                                    int oops = decompress(bytes2.toByteArray(), 1, x);
//                                    System.out.println(oops);
//                                }
//                                System.out.println(ds);
//
//                            } catch (IllegalAccessException e) {
//                                e.printStackTrace();
//                            } catch (IOException e) {
//                                e.printStackTrace();
//                            }printStackTrace
                            //bytes2.in

                            //return null;
                        }

                        @Override
                        public Binary visit(DataPageV2 dataPageV2) {
                            return null;
                        }
                    };


                    //                 simpleCol.readPage().accept(visi);
                    DictionaryPage dpp = simpleCol.readDictionaryPage();


                    long xx = rowGroup.getRowCount();
                    DictionaryPageReadStore dr = fr.getNextDictionaryReader();
// fr.getDictionaryReader(block).readDictionaryPage(columnDescriptor)
                    //DictionaryPage pg = fr.getDictionaryReader(block).readDictionaryPage(columnDescriptor);

                    Dictionary dictionary = dpp.getEncoding().initDictionary(columnDescriptor, dpp);
                    DictionaryValuesReader dvr = new DictionaryValuesReader(dictionary);
                    //DictionaryPageReader dpr = new DictionaryPageReader(reader, block)
                    //                DictionaryPageReadStore dpr = fr.getDictionaryReader( block);
                    //              System.out.println(dpr);
//dvr.initFromPage(pg.getDictionarySize(),);

                    for (int i = 0; i < dictionary.getMaxId(); i++) {
                        if (dictionary instanceof PlainValuesDictionary.PlainLongDictionary) {
                            Long l = dictionary.decodeToLong(i);
                            out.println("next dictionary value: " + l);
                        } else if (dictionary instanceof PlainValuesDictionary.PlainBinaryDictionary) {
                            Binary x = dictionary.decodeToBinary(i);
                            String g = x.toStringUsingUTF8();
                            out.println("next dictionary value: " + g);
                        }
                    }
                    out.println("start: " + block.getStartingPos() + "  end: "
                            + (block.getStartingPos()
                            + block.getCompressedSize())
                            + "  rows: " + block.getRowCount());


                }
            }

            long start = System.currentTimeMillis();
            // need to just read do calc ea row group not the whole file
            int n = 0;
            for (SimpleRecord value = reader.read(); value != null; value = reader.read()) {
                //n++;
                addRecord(value, "event");
                //System.out.println(value);
            }
            out.println("TT " + (System.currentTimeMillis() - start));

            out.println("values");
            for (Map.Entry<String, Set<String>> entry : nameValueSets.entrySet()) {
                out.println("values for [" + entry.getKey() + "]");
                for (String value : entry.getValue()) {
                    out.print(value + ",");
                }
                out.println();

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
            addValueFor(root + "-" + value.getName(), (String) value.getValue());
        } else {
            if (value.getValue() instanceof SimpleRecord) {
                SimpleRecord sm = (SimpleRecord) value.getValue();
                for (SimpleRecord.NameValue v : sm.getValues()) {

                    String s = "";
                    if (root.length() > 0) {
                        s = root;
                    }
                    if (!(v.getName().equalsIgnoreCase("bag") || v.getName().equalsIgnoreCase("array") || v.getValue() instanceof String)) {
                        s = s + "-" + v.getName();
                    }

                    addRecord(v, s);
                }
            }
        }
    }

    void addRecord(SimpleRecord value, String root) {
        for (SimpleRecord.NameValue nv : value.getValues()) {
            addRecord(nv, root);
        }
    }

    private void addValueFor(String s, String value) {
        out.println(s + "  " + value);


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
        if (t.isPrimitive()) {
            if (root.isEmpty()) {
                result.add(t.getName());
            } else
                result.add(root + "-" + t.getName());
        } else {
            GroupType gt = t.asGroupType();
            for (Type nt : gt.getFields()) {
                String rooty;
                if (root.isEmpty()) rooty = "";
                else rooty = root + "-";
                if (!gt.getName().equalsIgnoreCase("Bag") && !gt.getName().equalsIgnoreCase("Array"))
                    result.addAll(getf(nt, rooty + gt.getName()));
                else {
                    result.addAll(getf(nt, root));
                }
            }
        }
        return result;

    }
}



