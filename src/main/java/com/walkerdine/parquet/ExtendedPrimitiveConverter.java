package com.walkerdine.parquet;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.PrimitiveConverter;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by JEGreen1 on 29/03/2017.
 */


class ExtendedPrimitiveConverter extends PrimitiveConverter {


    private  List<String> actualValues = new ArrayList<String>();

    public ExtendedPrimitiveConverter(List<String> actualValues) {
        this.actualValues = actualValues;
    }

//    public ExtendedPrimitiveConverter(QuickGroupConverter quickGroupConverter, int i) {
//
//    }


    @Override
    public void addBinary(Binary value) {
        actualValues.add(value.toStringUsingUTF8());
        System.out.println("nxt non dict value" + value.toStringUsingUTF8());
    }

    @Override
    public void addBoolean(boolean value) {
        System.out.println("nxt non dict value" + value);
        actualValues.add(new Boolean(value).toString());
    }

    @Override
    public void addDouble(double value) {
        System.out.println("nxt non dict value" + value);
        actualValues.add(Double.toString(value));
    }

    @Override
    public void addFloat(float value) {
        System.out.println("nxt non dict value" + value);
    }

    @Override
    public void addInt(int value) {
        System.out.println("nxt non dict value" + value);
        actualValues.add(Integer.toString(value));

    }

    @Override
    public void addLong(long value) {
        System.out.println("nxt non dict value" + value);
        actualValues.add(Long.toString(value));

    }


}
