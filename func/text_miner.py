import re
import numpy as np
import pandas as pd

import spacy
from spacy.matcher import PhraseMatcher

nlp = spacy.load('en_core_web_sm')


def exclude_words_high_freq(df_in, thrst, do_plt=False, verbose=0):
    df = df_in.describe().T
    df['r'] = df['mean'] / df['std']    
    if do_plt: 
        df['r'].plot()
        (df['r']*0+thrst).plot()

    exclds = df_in.columns[df['r'] > thrst]

    if len(exclds) > 0:
        if verbose: print('EXCLUDING ' + exclds + ' due to high occurance everywhere')
        df_in = df_in.drop(exclds, axis=1)
    
    return df_in



def get_nbest(df_in, n=5, weigths=None):
    df = df_in.copy()
    if weigths is not None:
        for col, w in zip(df.columns, weigths):
            df[col] *= w
    
    return list(np.argsort(df.T.sum().T)[::-1][:n])
    
    

def standard_preproc_req(req_txt, exclude_words = ["mtm", "shall"], include_words=["off"], min_length=2):
    
    req_txt = req_txt.replace("DS", "")

    req_txt = req_txt.lower()  
    req_txt = re.sub('[^a-zA-Z]', ' ', req_txt )  
    req_txt = re.sub(r'\s+', ' ', req_txt)
    req_txt = ' '.join([w for w in req_txt.split() if len(w) >= min_length and (w in include_words or not (nlp.vocab[w].is_stop))])
    
    for exc in exclude_words:
        req_txt = req_txt.replace(exc, "")

    return list(set(req_txt.split()))


def standard_preproc_txt(article_text):
    processed_article = article_text.lower()  
    processed_article = re.sub('[^a-zA-Z]', ' ', processed_article )  
    processed_article = re.sub(r'\s+', ' ', processed_article)

    processed_article = ' '.join([w for w in processed_article.split() if not nlp.vocab[w].is_stop])
    return processed_article


def get_word_matches_cnt(pages_dict, words_dc, pre_proc_fun=None):
    if isinstance(words_dc, dict):
        fun = lambda words: get_word_matches_cnt(pages_dict, words, pre_proc_fun)
        return { wi:fun(words) for wi, words in words_dc.items() }
    
    elif isinstance(words_dc, str):
        words = words_dc.split()
    else:
        words = words_dc

    assert isinstance(words_dc, list), "words must come as list!"

    matches_page_cnt = {}
    for page_no, article_text in pages_dict.items(): 
        processed_article = article_text if pre_proc_fun is None else pre_proc_fun(article_text)
        matches_page_cnt[page_no] = [processed_article.count(w) for w in words]

    df = pd.DataFrame(matches_page_cnt.values(), index=matches_page_cnt.keys(), columns=words)
    return df


def get_word_matches_nlp(pages_dict, words_dc, pre_proc_fun=None):

    cnts_dc = {}
    lut_words_dc = {}
    lut_pages_dc = {ip:i for i, ip in enumerate(pages_dict.keys())}
    
    phrase_matcher = PhraseMatcher(nlp.vocab)
    
    for iw, words in words_dc.items(): 
        cnts_dc[iw] = np.zeros((len(pages_dict), len(words)))
        lut_words_dc[iw] = {w:i for i, w in enumerate(words)}
        phrase_matcher.add(iw, [nlp(w) for w in words])

    for page_no, article_text in pages_dict.items(): 
        processed_article = article_text if pre_proc_fun is None else pre_proc_fun(article_text)
        
        sentence = nlp(processed_article)
        matched_phrases = phrase_matcher(sentence)
        matched_words = {nlp.vocab.strings[match_id]:str(sentence[start:end].text) for match_id, start, end in matched_phrases}


        for match_id, match_word in matched_words.items():  
            iw = lut_words_dc[match_id][match_word]
            ip = lut_pages_dc[page_no]
            print(match_id, match_word, (ip, iw))
            cnts_dc[match_id][ip, iw] += 1

    ret = {iw:pd.DataFrame(cnts_dc[iw], index=pages_dict.keys(), columns=words_dc[iw]) for iw in words_dc.keys()}
    return ret


def get_word_matches_nlp_test():

    txt = {
        1: "a duck went swimming in a pool.",
        2: "the duck had a lot of fun in the pool"
    }

    words_dc = {
        'verbs': "went swimming".split(),
        'nouns': "duck pool".split()
    }

    matches_dfs = get_word_matches_nlp(txt, words_dc)
    print(matches_dfs)

def get_word_matches_cnt_test():

    txt = {
        1: "a duck went swimming in a pool.",
        2: "the duck had a lot of fun in the pool"
    }

    words_dc = {
        'verbs': "went swimming".split(),
        'nouns': "duck pool".split()
    }

    matches_dfs = get_word_matches_cnt(txt, words_dc)
    print(matches_dfs)
