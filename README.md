# Gutenberg Big Data Analytics: MapReduce & PySpark

This repository contains a two-part analysis of the Project Gutenberg dataset, transitioning from traditional Java-based MapReduce to modern distributed processing with Apache Spark. 

This project was developed as part of the M.Tech in Data Engineering curriculum at **IIT Jodhpur**.

## 🚀 Project Overview
The analysis processes a collection of ~200 ebooks to extract metadata, calculate semantic document similarity, and construct a temporal influence network.

---

## 1. Java MapReduce: `WordCount.java`
A foundational implementation of the WordCount algorithm using the Hadoop MapReduce framework.

### **Features**
* **Mapper:** Tokenizes input text and emits `(word, 1)` pairs.
* **Reducer:** Aggregates counts to produce a final frequency distribution.
* **Combiner:** Optimized to reduce network traffic between Map and Reduce phases by performing local aggregation.

# Project Gutenberg: Distributed Text Analytics with Apache Spark

This project implements a scalable Natural Language Processing (NLP) and Network Analysis pipeline using **Apache Spark**. It analyzes a corpus of ebooks to extract metadata, determine semantic document similarity, and model the temporal influence between authors.

## 🛠️ Tech Stack & Architecture
* **Engine:** Apache Spark 3.5.x (PySpark)
* **Data Structure:** Spark DataFrames & Sparse Vectors
* **Machine Learning:** Spark MLlib (TF-IDF, Normalizer)
* **Techniques:** Regular Expressions, Cosine Similarity, Directed Acyclic Graphs (DAG)

---

## 📊 Analysis Modules

### 1. Automated Metadata Extraction
Using Spark's `regexp_extract`, the pipeline parses unstructured Project Gutenberg headers to create a structured schema:
* **Fields:** Title, Author, Year, Language.
* **Data Cleaning:** Implemented casting and filtering to handle inconsistent date formats and missing values across the 184MB dataset.

### 2. Semantic Similarity Pipeline (TF-IDF)
To compare documents, the project builds a weighted numerical representation of each book:
* **Preprocessing:** Lowercasing, punctuation removal, and stop-word filtering using `StopWordsRemover`.
* **Vectorization:** Word frequencies are scaled using **Inverse Document Frequency (IDF)** to highlight unique vocabulary.
* **Memory Optimization:** A standard $O(N^2)$ Cartesian join was replaced with a **Broadcasted Dot-Product UDF** to calculate Cosine Similarity for a target book without crashing the Java Heap Space.

### 3. Temporal Influence Network
A directed network was constructed to model how authors potentially influenced one another:
* **Logic:** A directed edge exists if Author B published in the same language within a **1-year window** after Author A.
* **Key Findings:** Identified high **Out-Degree Centrality** for authors like G.K. Chesterton and identified "Bridge Authors" using network topology.

---
