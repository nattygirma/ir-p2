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
import random
import flask
from flask import Flask
from flask import request
import hashlib

app = Flask(__name__)


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


    def run_queries(self, queries_list, random_command="Hello"):
        """ DO NOT CHANGE THE output_dict definition"""
        output_dict = {'postingsList': {},
                    'postingsListSkip': {},
                   'daatAnd': {},
                   'daatAndSkip': {},
                   'daatAndTfIdf': {},
                   'daatAndSkipTfIdf': {},
                   'sanity': self.sanity_checker(random_command)}
        
        sorted_queries_list = sorted(queries_list)

        for query in tqdm(sorted_queries_list):
            # 1. Pre-process & tokenize the query
            input_term_arr = self.preprocessor.preprocess_query(query)

            # Initialize lists for storing postings and skip postings
            daat_and_result = None
            daat_and_skip_result = None
            daat_and_skip_result_l = None

            daat_and_comparisons = 0
            daat_and_skip_comparisons = 0

            for term in input_term_arr:
                # 2. Retrieve postings list and skip postings list for each term
                postings_list = self.get_postings_list(term)
                skip_postings_list = self.get_skip_postings_list(term)
                skip_postings_linkedList = self.get_skip_postings_linkedList(term)

                if postings_list is None:
                    postings_list = []  # Handle missing term by setting an empty list
                if skip_postings_list is None:
                    skip_postings_list = []
                if skip_postings_linkedList is None:
                    skip_postings_linkedList = LinkedList()
            # Store postings list and skip postings list in the output dict
                output_dict['postingsList'][term] = [doc_id for doc_id, _ in postings_list]
                output_dict['postingsListSkip'][term] = [doc_id for doc_id, _ in skip_postings_list]
            
                # 3. For DAAT AND operation, intersect postings list and skip postings list
                if daat_and_result is None:
                    daat_and_result = postings_list
                else:
                    daat_and_result, comparisons = self.daat_and(daat_and_result, postings_list)
                    daat_and_comparisons += comparisons

                if daat_and_skip_result_l is None:
                    daat_and_skip_result_l = skip_postings_linkedList
                else:
                   daat_and_skip_result_l, comparisons1 = self.daat_and_skip(daat_and_skip_result_l, skip_postings_linkedList)
                   daat_and_skip_comparisons += comparisons1
                daat_and_skip_result = self.linkedlist_to_array(daat_and_skip_result_l)
          
            # 4. Sort DAAT AND results by TF-IDF (if needed)
            daat_and_tfidf_result = self.sort_by_tfidf(daat_and_result)
            daat_and_skip_tfidf_result = self.sort_by_tfidf(daat_and_skip_result)
            # daat_and_skip_tfidf_result = self.sort_by_tfidf(daat_and_result)

            # Calculate number of docs in results
            num_docs_daat = len(daat_and_result)
            # num_docs_daat_skip = len(daat_and_result)
            num_docs_daat_skip = len(daat_and_skip_result)
            num_docs_daat_tfidf = len(daat_and_tfidf_result)
            num_docs_daat_skip_tfidf = len(daat_and_skip_tfidf_result)

            # Store the DAAT AND results and their tf-idf sorted versions with the required format
            output_dict['daatAnd'][query] = {
                'num_comparisons': daat_and_comparisons,
                'num_docs': num_docs_daat,
                'results': [doc_id for doc_id, _ in daat_and_result]
            }

            output_dict['daatAndSkip'][query] = {
                'num_comparisons': daat_and_skip_comparisons,
                'num_docs': num_docs_daat_skip,
                'results': [doc_id for doc_id, _ in daat_and_skip_result]
            }

            output_dict['daatAndTfIdf'][query] = {
                'num_comparisons': daat_and_comparisons,  # Same as DAAT AND
                'num_docs': num_docs_daat_tfidf,
                'results': [doc_id for doc_id, _ in daat_and_tfidf_result]
            }

            output_dict['daatAndSkipTfIdf'][query] = {
                'num_comparisons': daat_and_skip_comparisons,  # Same as DAAT AND with Skip
                'num_docs': num_docs_daat_skip_tfidf,
                'results': [doc_id for doc_id, _ in daat_and_skip_tfidf_result]
            }

        output_dict['postingsList'] = dict(sorted(output_dict['postingsList'].items()))
        
        output_dict['postingsListSkip'] = dict(sorted(output_dict['postingsListSkip'].items()))
        

        return output_dict
    
    def get_postings_list(self, term):
        """Retrieve the postings list for a term."""
        return self.indexer.inverted_index.get(term, None).traverse() if term in self.indexer.inverted_index else None
    def get_skip_postings_linkedList(self, term):
       """Retrieve the LinkedList with skip pointers for a term in Linkedlist format."""
       # Return the LinkedList directly from the inverted index if the term exists
       return self.indexer.inverted_index.get(term, None) if term in self.indexer.inverted_index else None
    
    def get_skip_postings_list(self, term):
        """Retrieve the postings list for a term in array format."""
        return self.indexer.inverted_index.get(term, None).traverse_with_skips() if term in self.indexer.inverted_index else None
    
    def linkedlist_to_array(self, linked_list: LinkedList):
        """Convert a LinkedList to an array of tuples (doc_id, term_freq)."""
        array_result = []
        current_node = linked_list.head
        while current_node is not None:
           # Append (doc_id, term_freq) to the result array
           array_result.append((current_node.doc_id, current_node.tf_idf))
           current_node = current_node.next

        return array_result
    def daat_and(self, postings1, postings2):
        """Perform DAAT AND operation on two postings lists."""
        result = []
        comparisons = 0
        i, j = 0, 0

        while i < len(postings1) and j < len(postings2):
             comparisons += 1  # Increment comparisons for each comparison
             doc1 = postings1[i][0]
             doc2 = postings2[j][0]
             
             if doc1 == doc2:
                
                result.append(postings1[i])  # Add doc_id to result if they match
                i += 1
                j += 1
             elif doc1 < doc2:
                 i += 1  # Move p1 to the next node
             else:
                j += 1  # Move p2 to the next node

        return result, comparisons


    def daat_and_skip(self, postings1: LinkedList, postings2: LinkedList):
        """Perform DAAT AND operation with skip pointers on LinkedList objects."""
        result = LinkedList()  # Use a LinkedList to store the result
        comparisons = 0
 
        p1 = postings1.head
        p2 = postings2.head
 
        while p1 is not None and p2 is not None:
            comparisons += 1
            doc1 = p1.doc_id
            doc2 = p2.doc_id

            # print(doc1 , " and ", doc2)

            if doc1 == doc2:
             
                # If both doc IDs match, add to result and move both pointers
                result.insert(p1.doc_id, p1.term_freq, p1.tf, p1.tf_idf)
                p1 = p1.next
                p2 = p2.next
            elif doc1 < doc2:
                # Check if we can skip in postings1
                if p1.skip and p1.skip.doc_id <= doc2:
                    # Move using skip pointers in postings1 as long as skip target is <= doc2
                    while p1.skip and p1.skip.doc_id <= doc2:
              
                        p1 = p1.skip  # Move to the skip target
                else:
                    # Move to the next posting if no skip is useful
                    p1 = p1.next
            else:
                # Check if we can skip in postings2
                if p2.skip and p2.skip.doc_id <= doc1:
                    # Move using skip pointers in postings2 as long as skip target is <= doc1
                    while p2.skip and p2.skip.doc_id <= doc1:
                     
                        p2 = p2.skip  # Move to the skip target
                else:
                    # Move to the next posting if no skip is useful
                    p2 = p2.next

        return result, comparisons
    def sort_by_tfidf(self, postings_list):
        """Sort the postings list by TF-IDF score, return empty list if None."""
        if postings_list is None:
            return []  # If postings_list is None, return an empty list
        return sorted(postings_list, key=lambda x: x[1], reverse=True)  # Assumin
    # 
    
    def sanity_checker(self, command):
        """ DO NOT MODIFY THIS. THIS IS USED BY THE GRADER. """

        index = self.indexer.get_index()
        kw = random.choice(list(index.keys()))
        return {"index_type": str(type(index)),
                "indexer_type": str(type(self.indexer)),
                "post_mem": str(index[kw]),
                "post_type": str(type(index[kw])),
                "node_mem": str(index[kw].start_node),
                "node_type": str(type(index[kw].start_node)),
                "node_value": str(index[kw].start_node.value),
                "command_result": eval(command) if "." in command else ""}


# def index(corpus):
#     projectRunner = ProjectRunner()
#     projectRunner.run_indexer(corpus)

#         #  user_input = input("Please enter your query: ")
#     user_input = [
#         "hello world",
#         "hello swimming",
#         "swimming going",
#         "random swimming"
#     ]
#     output = projectRunner.run_queries(user_input)
#     print("Output:",output)
# index("check.txt")

@app.route("/execute_query", methods=['POST'])
def execute_query():
    """ This function handles the POST request to your endpoint.
        Do NOT change it."""
    start_time = time.time()

    queries = request.json["queries"]
    random_command = request.json["random_command"]

    """ Running the queries against the pre-loaded index. """
    output_dict = runner.run_queries(queries, random_command)

    """ Dumping the results to a JSON file. """
    with open(output_location, 'w') as fp:
        json.dump(output_dict, fp)

    response = {
        "Response": output_dict,
        "time_taken": str(time.time() - start_time),
        "username_hash": username_hash
    }
    return flask.jsonify(response)



if __name__ == "__main__":
    """ Driver code for the project, which defines the global variables.
        Do NOT change it."""

    output_location = "project2_output.json"
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--corpus", type=str, help="Corpus File name, with path.")
    parser.add_argument("--output_location", type=str, help="Output file name.", default=output_location)
    parser.add_argument("--username", type=str,
                        help="Your UB username. It's the part of your UB email id before the @buffalo.edu. "
                             "DO NOT pass incorrect value here")

    argv = parser.parse_args()


    # corpus = argv.corpus
    # output_location = argv.output_location
    # username_hash = hashlib.md5(argv.username.encode()).hexdigest()

    corpus = "input_corpus.txt"
    output_location = "output.json"
    username_hash = hashlib.md5("natnaelg".encode()).hexdigest()



    """ Initialize the project runner"""
    runner = ProjectRunner()

    """ Index the documents from beforehand. When the API endpoint is hit, queries are run against 
        this pre-loaded in memory index. """
    runner.run_indexer(corpus)

    app.run(host="0.0.0.0", port=9999)