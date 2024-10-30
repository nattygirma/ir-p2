import re
from tqdm import tqdm
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords
import nltk

nltk.download('stopwords')

class Preprocessor:
    def __init__(self):
        self.stop_words = set(stopwords.words('english'))  # Load stopwords
        self.ps = PorterStemmer()  # Initialize Porter Stemmer

    def get_doc_id(self, doc):
        """ Splits each line of the document into doc_id & text.
            Already implemented"""
        arr = doc.split("\t")
        return int(arr[0]), arr[1]

    def tokenizer(self, text):
        """ Pre-process & tokenize document text.
            This method should be reusable for processing both documents and user queries.
        """

        # a. Convert to lowercase
        text = text.lower()

        # b. Remove special characters, only alphabets, numbers, and spaces should remain
        text = re.sub(r'[^a-z0-9\s]', ' ', text)  # Replace non-alphanumeric characters with spaces

        # c. Remove excess whitespace (multiple spaces to a single space)
        text = re.sub(r'\s+', ' ', text).strip()  # Collapse multiple spaces and trim the text

        # d. Tokenize the document using whitespace as a separator
        tokens = text.split()

        # e. Remove stopwords
        tokens = [word for word in tokens if word not in self.stop_words]

        # f. Perform Porter’s stemming on the tokens
        tokens = [self.ps.stem(word) for word in tokens]

        return tokens
    
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
        tokens = [self.ps.stem(token) for token in tokens]

        return tokens
    
# Function to process the file
# def process_file(file_path):
#     preprocessor = Preprocessor()

#     with open(file_path, 'r', encoding='utf-8') as file:
#         # print(file)
#         for line in file:
           
#             # Split into doc_id and text
#             doc_id, text = preprocessor.get_doc_id(line)
            
#             # Preprocess and tokenize the text
#             tokens = preprocessor.tokenizer(text)
            
#             # Output the results
#             print(f"Doc ID: {doc_id}, Tokens: {tokens}")

# # Call the function to process the file
# process_file('small_text.txt')