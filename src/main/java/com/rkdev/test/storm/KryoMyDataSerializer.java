package com.rkdev.test.storm;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoMyDataSerializer extends Serializer<MyData> {

	@Override
	public MyData read(Kryo arg0, Input input, Class<MyData> arg2) {
		MyData mydata = new MyData();
		mydata.salary = input.readDouble();
		mydata.name = "KRYO";
		mydata.age = 32;
		mydata.company = "KRYO";
		return mydata;
	}

	@Override
	public void write(Kryo arg0, Output output, MyData arg2) {
		output.writeDouble(arg2.salary);
	}

}
