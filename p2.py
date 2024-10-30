import re
from tqdm import tqdm
from preprocessor import Preprocessor
from indexer import Indexer
from collections import OrderedDict
from linkedList import LinkedList
import inspect as inspector
import sys
import argparse
import json
import time

class ProjectRunner:
    def __init__(self):
        self.preprocessor = Preprocessor()
        self.indexer = Indexer()


    def run_indexer(self, corpus):
        """ This function reads & indexes the corpus. After creating the inverted index,
            it sorts the index by the terms, add skip pointers, and calculates the tf-idf scores.
            Already implemented, but you can modify the orchestration, as you seem fit."""
        with open(corpus, 'r', encoding='utf-8') as file:
        # print(file)
            for line in file:
                doc_id, document = self.preprocessor.get_doc_id(line)
                tokenized_document = self.preprocessor.tokenizer(document)
                # print("tokenized documents",tokenized_document)
                self.indexer.generate_inverted_index(doc_id, tokenized_document)
        self.indexer.sort_terms()
        self.indexer.add_skip_connections()
        self.indexer.calculate_tf_idf()
        print("Indexing Complete")
        for term, posting_list in self.indexer.inverted_index.items():
        # posting_list.build_skip_pointers()
             postings = posting_list.traverse()
             docFreq= posting_list.docFreq
            #  print(f"Term: `{term}`, is in {docFreq} Documents, Postings: {postings}")



    def preprocess_query(self, query):
        """Preprocess the query by applying the same preprocessing steps used for documents."""
            # a. Convert to lowercase
        query = query.lower()

        # b. Remove special characters (replace with space)
        query = re.sub(r'[^a-z0-9\s]', ' ', query)

        # c. Remove excess whitespaces
        query = re.sub(r'\s+', ' ', query).strip()

        # d. Tokenize by splitting on whitespace
        tokens = query.split()

        # e. Remove stop words
        tokens = [token for token in tokens if token not in self.stop_words]

        # f. Perform Porter’s stemming
        tokens = [self.stemmer.stem(token) for token in tokens]

        return tokens

    def run_queries(self, query, random_command="command1"):
        """ DO NOT CHANGE THE output_dict definition"""
        output_dict = {'postingsList': {},
                       'postingsListSkip': {},
                       'daatAnd': {},
                       'daatAndSkip': {},
                       'daatAndTfIdf': {},
                       'daatAndSkipTfIdf': {},
                       'sanity': self.sanity_checker(random_command)}

        if(True):
        # for query in tqdm(query_list):
            # 1. Pre-process & tokenize the query
            input_term_arr = self.preprocessor.preprocess_query(query)

            # Initialize lists for storing postings and skip postings
            daat_and_result = None
            daat_and_skip_result = None
            print(f"Preprocessed Query ${input_term_arr}")

            for term in input_term_arr:
                # 2. Retrieve postings list and skip postings list for each term
                postings_list = self.get_postings_list(term)
                skip_postings_list = self.get_skip_postings_list(term)

                if postings_list is None:
                    postings_list = []  # No results if term not found
                if skip_postings_list is None:
                    skip_postings_list = []

                # 3. For DAAT AND operation, intersect postings list and skip postings list
                if daat_and_result is None:
                    daat_and_result = postings_list
                else:
                    daat_and_result = self.daat_and(daat_and_result, postings_list)

                if daat_and_skip_result is None:
                    daat_and_skip_result = skip_postings_list
                else:
                    daat_and_skip_result = self.daat_and_skip(daat_and_skip_result, skip_postings_list)

                # Store postings list and skip postings list in the output dict
                output_dict['postingsList'][term] = postings_list
                output_dict['postingsListSkip'][term] = skip_postings_list

            # 4. Sort DAAT AND results by TF-IDF (if needed)
            daat_and_tfidf_result = self.sort_by_tfidf(daat_and_result)
            daat_and_skip_tfidf_result = self.sort_by_tfidf(daat_and_skip_result)

            # Store the DAAT AND results and their tf-idf sorted versions
            output_dict['daatAnd'][query] = daat_and_result
            output_dict['daatAndSkip'][query] = daat_and_skip_result
            output_dict['daatAndTfIdf'][query] = daat_and_tfidf_result
            output_dict['daatAndSkipTfIdf'][query] = daat_and_skip_tfidf_result

        return output_dict

    def get_postings_list(self, term):
        """Retrieve the postings list for a term."""
        return self.indexer.inverted_index.get(term, None).traverse() if term in self.indexer.inverted_index else None

    def get_skip_postings_list(self, term):
        """Retrieve the postings list with skip pointers for a term."""
        return self.indexer.inverted_index.get(term, None).traverse_with_skips() if term in self.indexer.inverted_index else None

    def daat_and(self, postings1, postings2):
        """Perform DAAT AND operation on two postings lists."""
        result = []
        i, j = 0, 0

        while i < len(postings1) and j < len(postings2):
            if postings1[i][0] == postings2[j][0]:
                result.append(postings1[i])
                i += 1
                j += 1
            elif postings1[i][0] < postings2[j][0]:
                i += 1
            else:
                j += 1
        return result

    def daat_and_skip(self, postings1, postings2):
        """Perform DAAT AND operation with skip pointers."""
        # Similar to daat_and, but incorporate skip pointers for faster traversal
        # Placeholder implementation, add skip pointer logic here
        return self.daat_and(postings1, postings2)

    def sort_by_tfidf(self, postings_list):
        """Sort the postings list by TF-IDF score, return empty list if None."""
        if postings_list is None:
            return []  # If postings_list is None, return an empty list
        return postings_list
    # sorted(postings_list, key=lambda x: x[1], reverse=True)  # Assumin
    
    def sanity_checker(self, command):
        """Placeholder for a sanity checker."""
        return True

def index(corpus):
    projectRunner = ProjectRunner()
    projectRunner.run_indexer(corpus)
    while(True):
         user_input = input("Please enter your query: ")
         output = projectRunner.run_queries(user_input)
         print("Output:",output)
index("input_corpus.txt")
