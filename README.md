# Breadcrumb

Breadcumb computes query-based explanations for missing answers in Apache Spark's DataFrames. It rewrites Spark's query plan to point at operators that prevent an expected, but missing answer from appearing in the result. Breadcrumb supports flat relational data formats such as CSV and nested data formats such as JSON, XML, or Apache Parquet. 

Find details about Breadcrumb on [arxiv.org](https://arxiv.org/abs/2103.07561).

## Usage
The entry point to computing explanations is the object:

```
de.uni_stuttgart.ipvs.provenance.nested_why_not.WhyNotProvenance 
```

It provides the method:
```
computeMSRs(dataFrame: DataFrame, whyNotTwig: Twig): DataFrame
```

This method takes a Spark DataFrame and a twig (tree-pattern) as input. The twig describes the missing answer in the provided dataFrame. It returns a DataFrame that holds sets of operators. The listed operators need modification to make the missing answer appear in the result. Each of the sets is an explanation, why the missing answer is absent from the result. 

## Build
This is a Maven project. Please have a look at the [Maven getting started guide](https://maven.apache.org/guides/getting-started/) on instructions about compiling and packaing this project.



## Schema alternatives
If you like to test the schema alternatives described in the referenced work, checkout the branch named:
```
schema-alternatives
```