package org.crunchy.code.generator;

import java.util.List;

import javax.lang.model.element.Modifier;

import org.crunchy.mdc.stream.FlatMapFunction;
import org.crunchy.mdc.stream.MapFunction;
import org.crunchy.mdc.stream.MapTupleFunction;
import org.crunchy.mdc.stream.PredicateSerializable;
import org.crunchy.mdc.stream.SortedComparator;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeVariableName;

public class FunctionGeneratorUtility {

	@SuppressWarnings("rawtypes")
	public static TypeSpec getMapFunction(Class incls,Class outclass,String statement) {
		TypeSpec mapFunction = TypeSpec.anonymousClassBuilder("")
			    .addSuperinterface(ParameterizedTypeName.get(MapFunction.class, incls,outclass))
			    .addMethod(MethodSpec.methodBuilder("apply")
			        .addModifiers(Modifier.PUBLIC)
			        .addParameter(incls, "value")
			        .returns(outclass)
			        .addStatement(statement)
			        .build())
			    .build();
		return mapFunction;

	}
	@SuppressWarnings("rawtypes")
	public static TypeSpec getFilterFunction(Class cls,String statement) {
		TypeSpec filterFunction = TypeSpec.anonymousClassBuilder("")
			    .addSuperinterface(ParameterizedTypeName.get(PredicateSerializable.class, cls))
			    .addMethod(MethodSpec.methodBuilder("test")
			        .addModifiers(Modifier.PUBLIC)
			        .addParameter(cls, "value")
			        .returns(boolean.class)
			        .addStatement(statement)
			        .build())
			    .build();
		return filterFunction;

	}
	@SuppressWarnings("rawtypes")
	public static TypeSpec getMapTupleFunction(Class cls,String statement) {
		ClassName mainClass = ClassName.get("org.crunchy.mdc.stream", "MapTupleFunction");
		ClassName stringClass = ClassName.get(String.class);
		ClassName tupleClass = ClassName.get(Tuple2.class);
		TypeSpec filterFunction = TypeSpec.anonymousClassBuilder("")
			    .addSuperinterface(ParameterizedTypeName.get(mainClass,ParameterizedTypeName.get(cls), ParameterizedTypeName.get(tupleClass, stringClass,stringClass)))
			    .addMethod(MethodSpec.methodBuilder("apply")
			        .addModifiers(Modifier.PUBLIC)
			        .addParameter(cls, "value")
			        .returns(Tuple2.class)
			        .addStatement(statement)
			        .build())
			    .build();
		return filterFunction;

	}
	@SuppressWarnings("rawtypes")
	public static TypeSpec getFlatMapFunction(Class incls,Class outcls,String statement) {
		TypeSpec filterFunction = TypeSpec.anonymousClassBuilder("")
			    .addSuperinterface(ParameterizedTypeName.get(FlatMapFunction.class, incls, outcls))
			    .addMethod(MethodSpec.methodBuilder("apply")
			        .addModifiers(Modifier.PUBLIC)
			        .addParameter(incls, "value")
			        .returns(ParameterizedTypeName.get(List.class,outcls))
			        .addStatement(statement)
			        .build())
			    .build();
		return filterFunction;

	}
	@SuppressWarnings("rawtypes")
	public static TypeSpec getComparator(Class incls,String statement) {
		TypeSpec filterFunction = TypeSpec.anonymousClassBuilder("")
			    .addSuperinterface(ParameterizedTypeName.get(SortedComparator.class, incls))
			    .addMethod(MethodSpec.methodBuilder("compare")
			        .addModifiers(Modifier.PUBLIC)
			        .addParameter(incls, "value1")
			        .addParameter(incls, "value2")
			        .returns(int.class)
			        .addStatement(statement)
			        .build())
			    .build();
		return filterFunction;

	}
	public static void main(String[] args) throws ClassNotFoundException {
		System.out.println(getMapFunction(String.class,String[].class,"return value.split(\",\");"));
		System.out.println(getFilterFunction(String[].class,"return value[2].equals(\"2\");"));
		System.out.println(getMapTupleFunction(String[].class,"return Tuple.tuple(value[8],Long.parseLong(value[14]));"));
		System.out.println(getFlatMapFunction(String[].class,String.class,"return Arrays.asList(value[8]+\"-\"+value[14]);"));
		System.out.println(getComparator(String.class,"return value1.compareTo(value2);"));
	}
}
