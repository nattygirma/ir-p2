from math import sqrt, floor

class Node:
    def __init__(self,value, doc_id, term_freq, tf, tf_idf):
        self.value = value
        self.doc_id = doc_id          # Document ID
        self.term_freq = term_freq    # Term Frequency in the document
        self.next = None  # Frequency of the term in the document
        self.tf = tf  # tf score
        self.tf_idf = tf_idf
        self.skip = None

class LinkedList:
    def __init__(self):
        self.start_node = None
        self.end_node = None
        self.head = None  # Head of the linked list
        self.docFreq = 0     # Number of documents in the postings list

    def insert(self, doc_id, term_freq, tf, tf_idf):
        """Insert a new document ID and term frequency into the linked list."""
        new_node = Node(doc_id,doc_id, term_freq, tf, tf_idf)
        if not self.head:
            self.head = new_node
            self.start_node = new_node
        else:
            current = self.head
            prev = None
            while current is not None and current.doc_id < doc_id:
                prev = current
                current = current.next
            if prev is None:
                # Insert at the head
                new_node.next = self.head
                self.head = new_node
            else:
                prev.next = new_node
                new_node.next = current
        self.docFreq += 1

    def build_skip_pointers(self):
        """Add skip pointers at regular intervals based on sqrt(size) to improve query efficiency."""
        L = self.docFreq
        if L <= 2:
            return  # No need for skip pointers if the list is too small

        skip_interval = round(sqrt(L))  # Calculate the skip interval as round(sqrt(L))
        num_skips = floor(sqrt(L))  # Calculate the number of skip pointers as floor(sqrt(L))

        # Adjust if the length is a perfect square
        if L == skip_interval * skip_interval:
            num_skips -= 1

        current = self.head
        count = 0
        prev_skip = None
        skip_count = 0

        # Traverse the list and assign skip pointers
        while current:
            if count % skip_interval == 0:
                if prev_skip:
                    prev_skip.skip = current  # Assign the skip pointer
                prev_skip = current
                skip_count += 1
            count += 1
            current = current.next

    def traverse(self):
        """Simple traversal of the list, returning doc_id and term_freq as pairs (tuples)."""
        current = self.head
        result = []
        while current:
            result.append((current.doc_id, current.tf_idf))  # Append tuple (doc_id, term_freq)
            current = current.next
        return result

    def traverse_with_skips(self):
        """Traverse the linked list using skip pointers."""
        current = self.head
        result = []
        if current.skip:
            result.append((current.doc_id, current.tf_idf))
        # current = current.next
        while current:
            
            if current.skip:
                # result.append((f"{current.doc_id} -> skips to {current.skip.doc_id} ,",current.term_freq))
                result.append((current.skip.doc_id, current.tf_idf))
            else: 
                # result.append((current.doc_id, current.term_freq))
                next
            current = current.next
        return result