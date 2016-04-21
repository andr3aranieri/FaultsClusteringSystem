# -*- coding: utf-8 -*-
"""
Created on Fri Jul 10 19:54:31 2015

@author: andrearanieri
"""

from nltk import tokenize
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer

class TextPreprocessManager:
    
    def preprocess (self, text):
        text = unicode(text, errors='replace')
        text = text.replace("\t", " ")
        text = text.replace("\r", " ")
        text = text.replace("\n", " ")
        text = text.replace("'", " ")
        text = text.strip('\t\n\r')

        #NLTK removing punctuation and numbers;
        tokenizer = tokenize.RegexpTokenizer(r'[-.?!,":;()|0-9 ]', gaps=True)            
        word_list = tokenizer.tokenize(text)        

        #NLTK removing english stop words and Stemming Words;
        stemmer = PorterStemmer()
        filtered_words = [stemmer.stem(w) for w in word_list if not w in stopwords.words('english') and w != '' and w != ' ' and w != '\n' and len(w) > 1]

        return ' '.join(filtered_words)
