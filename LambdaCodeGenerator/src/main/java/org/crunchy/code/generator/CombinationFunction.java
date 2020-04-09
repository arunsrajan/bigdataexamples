package org.crunchy.code.generator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;

public class CombinationFunction {
	static String[] labeltransformation = { "Map", "Filter", "FlatMap", "Join", "Leftouterjoin", "Rightouterjoin",
			"Union", "MapTuple", "MapTupleGroupByKey", "MapTupleSortByKey", "MapTupleReduceByKey", "Sorted",
			"Intersection", "Sample", "Peek" };

	static String[] functiontransformation = { "map(%MapFunction)", "filter(%FilterFunction)",
			"flatMap(%FlatMapFunction)", "join()", "leftOuterjoin()", "rightOuterjoin()", "union()",
			"mapTuple(%MapTupleFunction)", "mapTuple(%MapTupleFunction).groupByKey()",
			"mapTuple(%MapTupleFunction).sortByKey()", "mapTuple(%MapTupleFunction).reduceByKey((a,b)->a+b)",
			"sorted(%ComparatorFunction)", "intersection()", "sample(46361)", "peek(System.out::println)" };

	static String[] labeltransformationrequired = { "Map", "Filter", "FlatMap", "MapTuple", "MapTupleGroupByKey",
			"MapTupleReduceByKey", "Sorted", "Sample", "Peek" };
	static Map<Class, String> codeMap = new HashMap<>();
	static Map<Class, String> codeFilter = new HashMap<>();
	static Map<Class, String> codeMapTuple = new HashMap<>();
	static Map<Class, String> codeFlatMap = new HashMap<>();
	static Map<Class, String> codeComparator = new HashMap<>();

	@SuppressWarnings("rawtypes")
	public static void main(String[] args) throws IOException {

		String[] functiontransformationrequired = { "map(%MapFunction)", "filter(%FilterFunction)",
				"flatMap(%FlatMapFunction)", "mapTuple(%MapTupleFunction)", "mapTuple(%MapTupleFunction).groupByKey()",
				"mapTuple(%MapTupleFunction).reduceByKey((a,b)->a+ b,null)", "sorted(%ComparatorFunction)",
				"sample(46361)", "peek(System.out::println)" };

		codeMap.put(String.class, "return value.split(\",\")");
		codeMap.put(String[].class, "return value[8]+\"-\"+value[14]");
		codeMap.put(Tuple2.class, "return value");

		codeFilter.put(String.class,
				"return !value.split(\",\")[14].equals(\"NA\")&& !value.split(\",\")[14].equals(\"ArrDelay\")");
		codeFilter.put(String[].class, "return !value[14].equals(\"NA\")&& !value[14].equals(\"ArrDelay\")");
		codeFilter.put(Tuple2.class, "return true");

		codeMapTuple.put(String.class,
				"return (Tuple2<String,String>)Tuple.tuple(value.split(\",\")[8],value.split(\",\")[14])");
		codeMapTuple.put(String[].class, "return (Tuple2<String,String>)Tuple.tuple(value[8],value[14])");
		codeMapTuple.put(Tuple2.class, "return (Tuple2<String,String>)value");

		codeFlatMap.put(String.class, "return Arrays.asList(value)");
		codeFlatMap.put(String[].class, "return Arrays.asList(value[8]+\"-\"+value[14])");

		codeComparator.put(String.class, "return value1.compareTo(value2)");
		codeComparator.put(String[].class, "return value1[1].compareTo(value2[1])");
		codeComparator.put(Tuple2.class, "return value1.compareTo(value2)");

		String[] terminaloperation = { "foreach()", "count()", "collect()" };
		int numberofcombination = 4;
		StringBuilder builder = new StringBuilder();
		List<String> currenttrans = new ArrayList<>();
		StringBuilder currentcodetrans = new StringBuilder();
		int currentdepth = 1;
		StringBuilder generatedCode = new StringBuilder();
		StringBuilder generatedCodeGBK = new StringBuilder();
		BufferedReader reader = new BufferedReader(new FileReader("template.txt"));
		String line;
		while ((line = reader.readLine()) != null) {
			generatedCode.append(line);
			generatedCode.append("\n");
		}
		reader.close();
		reader = new BufferedReader(new FileReader("templategroupbykey.txt"));
		while ((line = reader.readLine()) != null) {
			generatedCodeGBK.append(line);
			generatedCodeGBK.append("\n");
		}
		reader.close();
		combination(generatedCode.toString(), generatedCodeGBK.toString(), functiontransformationrequired,
				terminaloperation, builder, currenttrans, currentcodetrans, currentdepth, numberofcombination);
		FileWriter fw = new FileWriter("./combination.txt");
		fw.write(builder.toString());
		fw.close();
	}

	public static void combination(String generatedcode, String generatedCodeGBK,
			String[] functiontransformationrequired, String[] terminaloperation, StringBuilder builder,
			List<String> currenttrans, StringBuilder currentcodetrans, int currentdepth, int depth) {
		Pattern pattern1 = Pattern.compile("MapTupleGroupByKey(.)+");
		Pattern filter1 = Pattern.compile("Filter(.)+");
		Pattern pattern2 = Pattern.compile("(.)+MapTupleGroupByKey");
		Pattern filter2 = Pattern.compile("(.)+Filter");
		Matcher matcher;
		boolean processedtestdata = false;
		if (currentdepth > depth) {
			StringBuilder codeBuilder = new StringBuilder();
			Class startclass = String.class;
			System.out.println(currentcodetrans.toString());
			for (String function : currenttrans) {
				if (function.equals(functiontransformationrequired[0])) {
					if (startclass == String.class) {
						codeBuilder.append(functiontransformationrequired[0].replace("%MapFunction",
								FunctionGeneratorUtility
										.getMapFunction(startclass, String[].class, codeMap.get(startclass))
										.toString()));
						startclass = String[].class;
					} else if (startclass == String[].class) {

						codeBuilder.append(
								functiontransformationrequired[0].replace("%MapFunction", FunctionGeneratorUtility
										.getMapFunction(startclass, String.class, codeMap.get(startclass)).toString()));
						startclass = String.class;

					}
					codeBuilder.append(".");
				} else if (function.equals(functiontransformationrequired[1])) {
					codeBuilder.append(
							functiontransformationrequired[1].replace("%FilterFunction", FunctionGeneratorUtility
									.getFilterFunction(startclass, codeFilter.get(startclass)).toString()));
					codeBuilder.append(".");
				} else if (function.equals(functiontransformationrequired[2])) {
					if (startclass == String.class) {
						codeBuilder.append(functiontransformationrequired[2].replace("%FlatMapFunction",
								FunctionGeneratorUtility
										.getFlatMapFunction(String.class, String.class, codeFlatMap.get(startclass))
										.toString()));
					} else if (startclass == String[].class) {
						codeBuilder.append(functiontransformationrequired[2].replace("%FlatMapFunction",
								FunctionGeneratorUtility
										.getFlatMapFunction(String[].class, String.class, codeFlatMap.get(startclass))
										.toString()));
						startclass = Tuple2.class;
					}
					codeBuilder.append(".");
				} else if (function.equals(functiontransformationrequired[3])
						|| function.equals(functiontransformationrequired[4])
						|| function.equals(functiontransformationrequired[5])) {
					if (function.equals(functiontransformationrequired[3])) {

						codeBuilder.append(
								functiontransformationrequired[3].replace("%MapTupleFunction", FunctionGeneratorUtility
										.getMapTupleFunction(startclass, codeMapTuple.get(startclass)).toString()));
					} else if (function.equals(functiontransformationrequired[4])) {
						codeBuilder.append(
								functiontransformationrequired[4].replace("%MapTupleFunction", FunctionGeneratorUtility
										.getMapTupleFunction(startclass, codeMapTuple.get(startclass)).toString()));

					} else if (function.equals(functiontransformationrequired[5])) {
						codeBuilder.append(
								functiontransformationrequired[5].replace("%MapTupleFunction", FunctionGeneratorUtility
										.getMapTupleFunction(startclass, codeMapTuple.get(startclass)).toString()));

					}
					startclass = Tuple2.class;
					codeBuilder.append(".");
				} else if (function.equals(functiontransformationrequired[6])) {
					codeBuilder.append(
							functiontransformationrequired[6].replace("%ComparatorFunction", FunctionGeneratorUtility
									.getComparator(startclass, codeComparator.get(startclass)).toString()));
					codeBuilder.append(".");
				} else {
					codeBuilder.append(function);
					codeBuilder.append(".");
				}
			}
			String label = currentcodetrans.toString();
			String processedCode;
			if (label.contains("MapTupleGroupByKey")) {
				processedCode = generatedCodeGBK.replace("%1", label);
			} else {
				processedCode = generatedcode.replace("%1", label);
			}
			String code = codeBuilder.toString().trim();
			if (code.endsWith("..")) {
				code = code.substring(0, code.length() - 1);
			}
			// code=code.substring(0,code.length()-1);
			if(label.equals("FilterFilter")) {
				System.out.println(label);
			}
			matcher = pattern1.matcher(label);
			processedtestdata = false;
			if (matcher.matches()) {
				processedCode = processedCode.replace("%3", "46361");
				processedtestdata = true;
			}

			matcher = filter1.matcher(label);
			if (matcher.matches() && !processedtestdata) {
				processedtestdata = true;
				processedCode = processedCode.replace("%3", "45957");
			}

			matcher = pattern2.matcher(label);
			if (matcher.matches() && !processedtestdata) {
				processedtestdata = true;
				processedCode = processedCode.replace("%3", "46361");
			}

			matcher = filter2.matcher(label);
			if (matcher.matches() && !processedtestdata) {
				processedtestdata = true;
				processedCode = processedCode.replace("%3", "46361");
			}

			if (!processedtestdata)
				processedCode = processedCode.replace("%3", "46361");
			builder.append(processedCode.replace("%2", code));
			builder.append("\n");

			return;
		}
		for (int i = 0; i < functiontransformationrequired.length; i++) {
			if (currentdepth <= depth) {
				currenttrans.add(functiontransformationrequired[i]);
				currentcodetrans.append(labeltransformationrequired[i]);
				combination(generatedcode, generatedCodeGBK, functiontransformationrequired, terminaloperation, builder,
						currenttrans, currentcodetrans, currentdepth + 1, depth);
				currenttrans.remove(currenttrans.size() - 1);
				if (currentcodetrans.length() > 0) {
					currentcodetrans.delete(currentcodetrans.lastIndexOf(labeltransformationrequired[i]),
							currentcodetrans.length());
				}
			}
		}
	}
}
