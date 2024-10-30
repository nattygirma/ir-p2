from linkedList import LinkedList
from collections import defaultdict, OrderedDict
from math import sqrt, log

class Indexer:
    def __init__(self):
        """ Add more attributes if needed"""
        self.inverted_index = OrderedDict({})  # Use OrderedDict to store terms and posting lists
        self.total_docs = 0  # Keep track of the total number of documents

    def get_index(self):
        """ Function to get the index."""
        return self.inverted_index

    def generate_inverted_index(self, doc_id, tokenized_document):
        """This function adds each tokenized document to the index."""
        term_freqs = defaultdict(int)
        total_tokens_on_this_doc = 0

        # Calculate term frequencies for the current document
        for term in tokenized_document:
            term_freqs[term] += 1
            total_tokens_on_this_doc +=1
        # Increment the total number of documents
        self.total_docs += 1

        # Add terms and their document ID and term frequency to the index
        for term, freq in term_freqs.items():
            tf = freq/total_tokens_on_this_doc
            self.add_to_index(term, doc_id, freq, tf)

    def add_to_index(self, term_, doc_id_, freq, tf):
        """Add a term and its document ID + frequency to the inverted index."""
        # Check if the term already exists, if not, initialize a new LinkedList
        if term_ not in self.inverted_index:
            self.inverted_index[term_] = LinkedList()
        
        # Insert the document ID and term frequency into the linked list for the term
        self.inverted_index[term_].insert(doc_id_, freq,tf,0)


    def sort_terms(self):
        """ Sorting the index by terms."""
        # self.inverted_index = OrderedDict(sorted(self.inverted_index.items()))
        """ Sorting the index by terms.
            Already implemented."""
        sorted_index = OrderedDict({})
        for k in sorted(self.inverted_index.keys()):
            sorted_index[k] = self.inverted_index[k]
        self.inverted_index = sorted_index

    def add_skip_connections(self):
        """ For each postings list in the index, add skip pointers."""
        for term, postings_list in self.inverted_index.items():
            postings_list.build_skip_pointers()

    def calculate_tf_idf(self):
        """ Calculate tf-idf score for each document in the postings lists of the index.
        total_docs: Total number of documents in the corpus
        doc_lengths: Dictionary mapping doc_id to total number of tokens in that document
        """
        for term, postings_list in self.inverted_index.items():
            doc_freq = postings_list.docFreq  # Number of documents containing the term
            idf = self.total_docs / doc_freq  # Inverse Document Frequency

            current_node = postings_list.head
            while current_node:
                tf = current_node.tf
                current_node.tf_idf = tf * idf  # Store the tf-idf score in the node
                current_node = current_node.next

